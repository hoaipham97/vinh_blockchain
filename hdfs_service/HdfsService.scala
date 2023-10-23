package gds.data.core.services

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.S3ClientOptions
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client}
import gds.data.core.utils.LoggerUtil
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, FileUtil, Path}
import org.apache.spark.sql.functions.{col, explode, udf}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.JavaConversions._
import java.io.PrintWriter
import java.net.URI
import java.nio.charset.StandardCharsets

object HdfsService extends LoggerUtil with FS {
  var hadoopConfig: Configuration = null

  def initHadoopConfig(hadoopConfig: Configuration): Unit = {
    this.hadoopConfig = new Configuration(hadoopConfig)
  }

  def getFs(path: String): FileSystem = {
    if (path != null && path.startsWith("s3a://")) {
      new Path(path).getFileSystem(hadoopConfig)
    } else {
      FileSystem.get(new Configuration)
    }
  }

  def getSchema(spark: SparkSession, format: String, path: String, options: Map[String, String] = Map.empty): StructType = {
    var df: DataFrame = spark.emptyDataFrame
    format match {
      case "parquet" =>
        df = spark.read.parquet(path)
      case "json" =>
        df = spark.read.json(path)
      case "csv" =>
        df = spark.read.options(options).csv(path)
      case "orc" =>
        df = spark.read.orc(path)
      case _ =>
        throw new Exception(s"Can not detect file format $format")
    }
    val schemaJsonString = df.schema.prettyJson
    val schema = DataType.fromJson(schemaJsonString).asInstanceOf[StructType]
    schema
  }

  def load(spark: SparkSession, fileType: String = "parquet", path: String, options: Map[String, String] = Map.empty): Option[DataFrame] = {
    loadMultiplePaths(spark, fileType, List(path), options)
  }

  def loadMultiplePaths(spark: SparkSession, fileType: String = "parquet", paths: List[String], options: Map[String, String] = Map.empty): Option[DataFrame] = {
    logger.info(s"Load data from ${paths.mkString(",")} with fileType=$fileType, option=${options.mkString(",")}")
    import spark.implicits._

    try {
      val dataFrame = fileType match {
        case "parquet" | "json" | "csv" | "orc" | "text" | "gds.data.core.sources.stream.api.puller.Puller" =>
          spark.read.format(fileType).options(options).load(paths: _*)
        case "iceberg" =>
          spark.read.format(fileType).options(options).load(paths.head)
        case "textPattern" =>
          spark.conf.set("spark.sql.shuffle.partitions", "1")
          val patterns = options("inputPattern").split(',')
          var prefixes = patterns
          spark.sparkContext.textFile(paths.mkString(",")).map(row => {
            var cnt = 0
            var text = ""
            if (options("encoding").nonEmpty) text = new String(row.getBytes(), options("encoding"))
            else text = row
            for (i <- patterns.indices) {
              if (text.matches(patterns(i))) prefixes = prefixes.updated(i, text)
              else cnt += 1
            }
            if (cnt == patterns.length) {
              cnt = 0
              prefixes = patterns
            }
            prefixes.mkString("\t")
          }).toDF().coalesce(1)
        case "textEncoded" =>
          val cvtString = udf((payload: Array[Byte]) => (new String(payload, StandardCharsets.ISO_8859_1)).split("\n"))
          spark.read.format("binaryFile").load(paths: _*).select(explode(cvtString(col("content"))).as("value"))

        case _ =>
          throw new Exception(s"Can not detect fileType $fileType")
      }
      if (dataFrame.head(1).isEmpty) {
        logger.error(s"${paths.mkString(",")} is empty with fileType=$fileType, option=${options.mkString(",")}")
        None
      } else {
        Some(dataFrame)
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Cannot load ${paths.mkString(",")} with fileType=$fileType, option=${options.mkString(",")} caused by: ${ex.getMessage}", ex.getCause)
        None
    }
  }

  def saveWithAutoCompact(sparkSession: SparkSession, fileType: String = "parquet", df: DataFrame, path: String, tmpDir: String, partitionColumn: String = "", mode: SaveMode = SaveMode.Overwrite, options: Map[String, String] = Map.empty): Unit = {
    val tempPath = s"${tmpDir}/auto_compact_tmp/${path.hashCode}/${System.currentTimeMillis()}"

    logger.info(s"Save data into temp path $tempPath with format=$fileType, option=${options.mkString(",")}, mode=$mode, partitionColumn=$partitionColumn")
    var writerTmp = df.coalesce(32).write.format(fileType).options(options).mode(mode)
    if (StringUtils.isNotBlank(partitionColumn)) {
      writerTmp = writerTmp.partitionBy(partitionColumn)
    }
    writerTmp.save(tempPath)
    logger.info(s"Compact data from temp path $tempPath to final path $path with format=$fileType, option=${options.mkString(",")}, mode=$mode, partitionColumn=$partitionColumn")
    // compact
    val dataSize = getSizeOfPath(tempPath)
    val numFiles = Math.ceil(dataSize.asInstanceOf[Double] / 128000000.0).asInstanceOf[Int]
    logger.info(s"Compact to $numFiles files, with dataSize $dataSize Bytes")
    var writer = sparkSession.read.format(fileType).options(options).load(tempPath).coalesce(numFiles).write
      .format(fileType).options(options).mode(mode)
    if (StringUtils.isNotBlank(partitionColumn)) {
      writer = writer.partitionBy(partitionColumn)
    }
    writer.save(path)
    logger.info(f"Done auto compact from temp path $tempPath to final path $path with format=$fileType, option=${options.mkString(",")}, mode=$mode, partitionColumn=$partitionColumn")
  }

  def save(fileType: String = "parquet", df: DataFrame, path: String, numPartitions: Int = 32, partitionColumn: String = "", mode: SaveMode = SaveMode.Overwrite, options: Map[String, String] = Map.empty): Unit = {
    logger.info(s"Save data into $path with format=$fileType, option=${options.mkString(",")}, mode=$mode, numPartition=$numPartitions, partitionColumn=$partitionColumn")
    var writer = df.coalesce(numPartitions).write.format(fileType).options(options).mode(mode)
    if (StringUtils.isNotBlank(partitionColumn)) {
      writer = writer.partitionBy(partitionColumn)
    }
    writer.save(path)
  }

  def saveWithSafeMode(spark: SparkSession, dataFrame: DataFrame, path: String, tempPath: String, numPartitions: Int = 32): Unit = {
    save("parquet", dataFrame, tempPath, numPartitions)

    val dataOpt = load(spark, "parquet", tempPath)
    if (dataOpt.isEmpty) {
      throw new Exception(s"Cannot load $tempPath")
    }

    save("parquet", dataOpt.get, path, numPartitions)
  }


  // only use this method to copy folder from s3 to s3

  import com.amazonaws.AmazonServiceException
  def doesObjectExists(cli: AmazonS3Client, bucket: String, key: String): Boolean = {
    try {
      cli.getObjectMetadata(bucket, key)
    } catch {
      case e: AmazonServiceException =>
        return false
    }
    true
  }

  def copyS3Folder(path: String, destPath: String, overwrite: Boolean = false): Unit = {
    val startTime = System.currentTimeMillis()
    val cli = new AmazonS3Client(new BasicAWSCredentials(hadoopConfig.get("fs.s3a.access.key"), hadoopConfig.get("fs.s3a.secret.key")))
    cli.setEndpoint(hadoopConfig.get("fs.s3a.endpoint"))
    cli.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true))
    val srcUri = new URI(path)
    val srcBucket = srcUri.getHost
    val srcPrefix = srcUri.getPath.stripPrefix("/").stripSuffix("/")

    val destUri = new URI(destPath)
    val destBucket = destUri.getHost
    val destPrefix = destUri.getPath.stripPrefix("/").stripSuffix("/")

    val objs = cli.listObjects(srcBucket, s"${srcPrefix}/").getObjectSummaries.toSeq
    logger.info(s"Copying objects: ${objs}")
    objs.foreach(obj => {
      val objName = obj.getKey.replace(s"${srcPrefix}/", "")
      val destObj = s"${destPrefix}/${objName}"
      logger.info(s"Copying ${obj.getKey} to ${destObj}")
      if (overwrite) {
        cli.copyObject(srcBucket, obj.getKey, destBucket, destObj)
      } else {
        if (!doesObjectExists(cli, destBucket, destObj)) {
          cli.copyObject(srcBucket, obj.getKey, destBucket, destObj)
        } else {
          throw new Exception(s"Failed to copy from ${obj.getKey} to ${destObj}. Reason ${destObj} already exists")
        }
      }
    })
  logger.info(s"Copy from ${path} to ${destPath} take ${System.currentTimeMillis() - startTime} ms")
  }

  def deleteFolder(path: String, backup: Boolean = false): Unit = {
    if (path.startsWith("s3a://")) {
      logger.info("Not implemented yet")
      throw new IllegalStateException("Not implemented yet")
    } else {
      val startDelete = System.currentTimeMillis()

      if (HdfsService.exists(path)) {
        val backupPath = path + "_backup"
        logger.info(s"Backing up $path to $backupPath")
        copyFolder(path, backupPath, overwrite = true)
        logger.info(s"Deleting $path")
        val success = HdfsService.delete(path)
        if (!success) {
          throw new Exception(f"Failed to copy path $path")
        } else {
          if(!backup){
            HdfsService.delete(backupPath)
          }
        }
        logger.info(f"Done delete $path take ${System.currentTimeMillis() - startDelete} ms")
      } else {
        logger.warn(f"$path doest not exist")
      }
    }
  }

  def copyFolder(path: String, destPath: String, overwrite: Boolean = false): Unit = {
    if (path.startsWith("s3a://") && destPath.startsWith("s3a://")) {
      copyS3Folder(path, destPath, overwrite)
    } else {
      val startCopy = System.currentTimeMillis()
      logger.info(s"Copying from $path to $destPath")
      logger.info(s"List file at srcPath: ${listFiles(path)}")
      val sourceFs = getFs(path)
      val destFs = getFs(destPath)

      if (HdfsService.exists(path)) {
        val success = FileUtil.copy(sourceFs, new Path(path), destFs, new Path(destPath), false, overwrite, destFs.getConf)
        if (!success) {
          throw new Exception(f"Failed to copy path ${path} to {$destPath}")
        }
        logger.info(f"Done copy from $path to $destPath take ${System.currentTimeMillis() - startCopy} ms")
      } else {
        logger.warn(f"$path doest not exist")
      }
    }
  }

  def getSizeOfPath(path: String): Long = {
    val fs = getFs(path)
    fs.getContentSummary(new Path(path)).getLength
  }

  def readStream(spark: SparkSession, fileType: String, path: String, options: Map[String, String] = Map.empty): Option[DataFrame] = {
    spark.conf.set("spark.sql.streaming.schemaInference", "true")
    try {
      val df = spark.readStream.format(fileType)
//        .option("maxFilesPerTrigger", 32)
        .options(options)
        .option("maxFileAge", 0)
        .load(path)
      Some(df)
    } catch {
      case ex: Throwable =>
        logger.error(ex.getMessage, ex.getCause)
        None
    }
  }

  def waitSuccessFlag(path: String): Unit = {
    logger.debug(s"WAITING $path")
    new Thread(new Runnable() {
      override def run(): Unit = {
        while (!exists(s"$path/_SUCCESS")) {
          Thread.sleep(1000)
        }
      }
    }).start()
  }

  def open(filePath: String): FSDataInputStream = {
    val fs = getFs(filePath)
    fs.open(new Path(filePath))
  }

  def touch(filePath: String): Unit = {
    val fs = getFs(filePath)
    fs.create(new Path(filePath), true).close()
  }

  def delete(filePath: String): Boolean = {
    val fs = getFs(filePath)
    if (fs.exists(new Path(filePath))) {
      fs.delete(new Path(filePath), true)
    } else {
      false
    }
  }

  // Supports to check file exists with pattern
  def exists(filePath: String): Boolean = {
    try {
      val fs = getFs(filePath)
      val statuses = fs.globStatus(new Path(filePath))
      if (statuses == null || statuses.isEmpty) {
        return false
      }

      for (x <- statuses) {
        if (fs.exists(x.getPath)) {
          return true
        }
      }

      false
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage, ex.getCause)
        false
    }
  }

  def listFiles(filePath: String, recursive: Boolean = false): List[String] = {
    try {
      val fs = getFs(filePath)
      val globPaths = globPathIfNecessary(new Path(filePath))
      val statuses = globPaths.flatMap(path => fs.listStatus(path))
      if (statuses == null || statuses.isEmpty) {
        logger.warn(s"Cannot get list status of `$filePath`")
        return List.empty
      }

      statuses.flatMap(status => {
        if (status.isFile) {
          List(status.getPath.toString)
        } else {
          if (recursive) listFiles(status.getPath.toString) else List.empty
        }
        //        else {
        //          listFiles(filePath + "/" + status.getPath.getName)
        //        }
      }).filter(_.nonEmpty).toList
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage, ex.getCause)
        List.empty
    }
  }

  def listSubDirectories(filePath: String): List[String] = {
    try {
      val fs = getFs(filePath)
      val globPaths = globPathIfNecessary(new Path(filePath))
      val statuses = globPaths.flatMap(path => fs.listStatus(path))
      if (statuses == null || statuses.isEmpty) {
        logger.warn(s"Cannot get list status of `$globPaths`")
        return List.empty
      }

      statuses.flatMap(status => {
        if (status.isDirectory) {
          List(status.getPath.toString)
        } else {
          List.empty
        }
      }).filter(_.nonEmpty).toList
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage, ex.getCause)
        List.empty
    }
  }

  def listParDirectories(filePath: String): List[String] = {
    try {
      val fs = getFs(filePath)
      val globPaths = globPathIfNecessary(new Path(filePath))
      val statuses = globPaths.flatMap(path => fs.globStatus(path))
      if (statuses == null || statuses.isEmpty) {
        logger.warn(s"Cannot get glob status of `$globPaths`")
        return List.empty
      }

      statuses.flatMap(status => {
        if (status.isDirectory) {
          List(status.getPath.toString)
        } else {
          List.empty
        }
      }).filter(_.nonEmpty).toList
    } catch {
      case ex: Exception =>
        logger.error("Error while listing directories: " + ex.getMessage)
        List.empty
    }
  }

  def listHDFSParDirectories(filePath: String): List[String] = {
    try {
      val fs = getFs(filePath)
      val globPaths = Seq(new Path(filePath))
      val statuses = globPaths.flatMap(path => fs.globStatus(path))
      if (statuses == null || statuses.isEmpty) {
        logger.warn(s"Cannot get glob status of `$globPaths`")
        return List.empty
      }

      statuses.flatMap(status => {
        if (status.isDirectory) {
          List(status.getPath.toString)
        } else {
          List.empty
        }
      }).filter(_.nonEmpty).toList
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage, ex.getCause)
        List.empty
    }
  }

  def read(filePath: String): String = {
    val inputStream = open(filePath)
    val value = IOUtils.toString(inputStream, "UTF-8")
    inputStream.close()
    value
  }

  protected def isGlobPath(pattern: Path): Boolean = {
    pattern.toString.exists("{}[]*?\\".toSet.contains)
  }

  protected def globPath(pattern: Path): Seq[Path] = {
    val fs = pattern.getFileSystem(hadoopConfig)
    globPath(fs, pattern)
  }

  protected def globPath(fs: FileSystem, pattern: Path): Seq[Path] = {
    Option(fs.globStatus(pattern)).map { statuses =>
      statuses.map(_.getPath.makeQualified(fs.getUri, fs.getWorkingDirectory)).toSeq
    }.getOrElse(Seq.empty[Path])
  }

  protected def globPathIfNecessary(pattern: Path): Seq[Path] = {
    if (isGlobPath(pattern)) globPath(pattern) else Seq(pattern)
  }
}
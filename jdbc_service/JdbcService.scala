package gds.data.core.services

import gds.data.core.configs.{DatabaseConfiguration, JobConfiguration, OutputDb}
import gds.data.core.services.JdbcService.getDriverClassName
import gds.data.core.services.databaseadapter.{Database, DbMySql, DbPostgresql}
import gds.data.core.utils.{JsonUtil, LoggerUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.sql.DriverManager

class JdbcService(outputDb: OutputDb, dbConfig: DatabaseConfiguration, spark: SparkSession) extends LoggerUtil {

  import spark.implicits._

  private lazy val db: Database = outputDb.dbType match {
    case "mysql" => new DbMySql(dbConfig, spark)
    case "postgres" => new DbPostgresql(dbConfig, spark)
    case _ => null
  }

  private def deleteByKey(df: DataFrame, tableName: String, key: Seq[String]): Unit = {
    var conditionDf: DataFrame = null
    if (key != null && key.nonEmpty) {
      conditionDf = df.select(key.head, key.tail: _*).distinct()
        .map(row => {
          var condition = ""
          key.zipWithIndex.map {
            case (key, i) =>
              condition = condition + s"$key = '${row.get(i).toString}' and "
              condition
          }
          s"delete from $tableName where (${condition.dropRight(5)})"
        }).toDF("condition").distinct()

      logger.info(s"Delete old data by key ${key.mkString(",")}")
    } else {
      logger.info(s"Truncate all data from table $tableName")
      conditionDf = Seq(s"truncate table $tableName").toDF("condition")
    }

    conditionDf.foreachPartition((part: Iterator[Row]) => {
      val conn = DriverManager.getConnection(db.dbUrl, db.dbUser, db.dbPass)
      conn.setAutoCommit(false)

      part.foreach(row => {
        val stmt = conn.createStatement()
        stmt.execute(row.mkString)
      })
      conn.commit()
      conn.close()
    })
  }

  /**
   * Insert data to db using spark method
   *
   * @param df        Input dataframe
   * @param tableName Table name
   * @param key       Table key, used to delete old data before inserting
   * @param saveMode  Save mode override/append (append by default)
   */
  def insertSpark(df: DataFrame, tableName: String, key: Seq[String], saveMode: SaveMode = SaveMode.Append): Unit = {
    Class.forName(db.dbDriver)
    deleteByKey(df, tableName, key)

    logger.info(s"Inserting data into table = $tableName")

    val dfToStore = if(outputDb.isTrackCreateTime){
      df.withColumn("created_time", current_timestamp())
    } else{
      df
    }

    dfToStore.coalesce(outputDb.numPartitions).write.format("jdbc").mode(saveMode)
      .option("driver", db.dbDriver)
      .option("url", db.dbUrl)
      .option("dbtable", tableName)
      .option("user", db.dbUser)
      .option("password", db.dbPass)
      .option("queryTimeout", db.dbStatementTimeout)
      .option("batchsize", "10000")
      .save()
  }

  /**
   * Insert data using java method
   *
   * @param df        Input data
   * @param tableName Table name
   */
  def insertJava(df: DataFrame, tableName: String): Unit = {
    try {
      db.insert(df, tableName)
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage, e.getCause)
        throw new Exception(e.getMessage, e.getCause)
    }
  }

  private def getUpsertStatement(df: DataFrame, tableName: String, updateKey: Seq[String]): String = {
    val fields = df.schema.map(_.name)
    val schemaStatement = fields.mkString("(", ",", ")")
    val insertValues = fields.map(_ => "?").mkString("(", ",", ")")
    val duplicatedStatement = updateKey.map(key => s"$key = excluded.$key").mkString(",")

    s"""
       |insert into $tableName $schemaStatement
       | values $insertValues as excluded on duplicate key update
       | $duplicatedStatement
       |""".stripMargin
  }

  def upsertSpark(df: DataFrame, tableName: String, updateKey: Seq[String]): Unit = {
    val statement = getUpsertStatement(df, tableName, updateKey)
    Class.forName(getDriverClassName(outputDb.dbType))
    lazy val conn = DriverManager.getConnection(dbConfig.url, dbConfig.user_dba, dbConfig.pass_dba)
    df.rdd.foreachPartition((rows: Iterator[Row]) => {
      val preparedStatement = conn.prepareStatement(statement)
      val updateStatement = rows.foldLeft(preparedStatement)((preparedStatement, row) => {
        for (i <- 0 until row.length) {
          preparedStatement.setObject(i + 1, row.get(i))
        }
        preparedStatement.addBatch()
        preparedStatement
      })
      updateStatement.executeBatch()
    })
    conn.close()
  }

  def read(spark: SparkSession, jobConfig: JobConfiguration, path: String): Option[DataFrame] = {
    val config = path.replace("jdbc://", "").split("/")
    val dbType = config.apply(0)
    val dbName = config.apply(1)
    val tblName = config.apply(2)
    val dbConfig = JsonUtil.parseFromFile[DatabaseConfiguration](s"${jobConfig.configDir}/db/$dbName.json")

    try {
      val df = spark.read.format("jdbc")
        .option("driver", getDriverClassName(dbType))
        .option("url", dbConfig.url)
        .option("dbtable", tblName)
        .option("user", dbConfig.user_dba)
        .option("password", dbConfig.pass_dba)
        .load()
      Some(df)
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage, e.getCause)
        None
    }
  }
}

object JdbcService extends LoggerUtil {
  val driver = "org.postgresql.Driver"

  def apply(outputDb: OutputDb, dbConfig: DatabaseConfiguration, spark: SparkSession): JdbcService = new JdbcService(outputDb, dbConfig, spark)

  def read(spark: SparkSession, jobConfig: JobConfiguration, path: String): Option[DataFrame] = {
    val config = path.replace("jdbc://", "").split("/")
    val dbType = config.apply(0)
    val dbName = config.apply(1)
    val tblName = config.apply(2)
    val dbConfig = JsonUtil.parseFromFile[DatabaseConfiguration](s"${jobConfig.configDir}/db/$dbName.json")

    try {
      val df = spark.read.format("jdbc")
        .option("driver", getDriverClassName(dbType))
        .option("url", dbConfig.url)
        .option("dbtable", tblName)
        .option("user", dbConfig.user_dba)
        .option("password", dbConfig.pass_dba)
        .load()
      Some(df)
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage, e.getCause)
        None
    }
  }

  def insert(spark: SparkSession, df: DataFrame, conf: DatabaseConfiguration, tableName: String, trigger: String = "", saveMode: SaveMode = SaveMode.Overwrite, db: String = "psql"): Unit = {
    try {
      df.write.format("jdbc").mode(saveMode)
        .option("driver", getDriverClassName(db))
        .option("url", conf.url)
        .option("dbtable", tableName)
        .option("user", conf.user_dba)
        .option("password", conf.pass_dba)
        .option("batchsize", "10000")
        .save()
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage, e.getCause)
        throw new Exception(e.getMessage, e.getCause)
    }

    if (StringUtils.isNotBlank(trigger)) {
      logger.info(s"Running sql: $trigger")
      Class.forName(driver)
      val conn = DriverManager.getConnection(conf.url, conf.user_dba, conf.pass_dba)
      val stmt = conn.createStatement()
      stmt.execute(trigger)
      conn.close()
    }
  }

  def upsert(df: DataFrame, outputDb: OutputDb, dbConfig: DatabaseConfiguration): Unit = {
    val statement = getUpsertStatement(df, outputDb.tableNameNew, outputDb.dataKeys.split(",").toSeq)
    df.coalesce(10).rdd.foreachPartition(partition => {
      partition.grouped(100).foreach(rows => {
        Class.forName(getDriverClassName(outputDb.dbType))
        val conn = DriverManager.getConnection(dbConfig.url, dbConfig.user_dba, dbConfig.pass_dba)
        val preparedStatement = conn.prepareStatement(statement)
        val updateStatement = rows.foldLeft(preparedStatement)((preparedStatement, row) => {
          for (i <- 0 until row.length) {
            preparedStatement.setObject(i + 1, row.get(i))
          }
          preparedStatement.addBatch()
          preparedStatement
        })
        updateStatement.executeBatch()
        conn.close()
      })
    })
  }

  private def getUpsertStatement(df: DataFrame, tableName: String, updateKey: Seq[String]): String = {
    val fields = df.schema.map(_.name)
    val schemaStatement = fields.mkString("(", ",", ")")
    val insertValues = fields.map(_ => "?").mkString("(", ",", ")")
    val duplicatedStatement = updateKey.map(key => s"$key = excluded.$key").mkString(",")

    s"""
       |insert into $tableName $schemaStatement
       | values $insertValues as excluded on duplicate key update
       | $duplicatedStatement
       |""".stripMargin
  }

  def getDriverClassName(db: String): String = {
    db match {
      case "psql" =>
        "org.postgresql.Driver"
      case "mysql" =>
        "com.mysql.cj.jdbc.Driver"
      case _ =>
        throw new Exception(s"Database $db is not supported")
    }
  }
}

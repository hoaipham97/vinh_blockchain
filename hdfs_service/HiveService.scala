package gds.data.core.services

import gds.data.core.configs.Column
import gds.data.core.utils.LoggerUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveService extends LoggerUtil() {

  val HIVE_WAREHOUSE = "/apps/hive/warehouse"

  private def applyPartitionColumn(partitionColumns: Array[Column], logDate: String): Array[Column] = {
    val columnApplied = partitionColumns.map(pColumn => {
      val newCol = pColumn
      if (logDate != null) {
        newCol.colValue = newCol.colValue.toString.replace("{logDate}", logDate)
      }
      newCol
    })

    columnApplied
  }

  /**
   * Copy data to hive warehouse dir
   *
   * @param sourcePath       Data path
   * @param schemaName       Hive schema name
   * @param tableName        Hive table name
   * @param partitionColumns Array of partition column. Empty by default
   * @param isBackup         Need backup? False by default
   * @return Data storage path on Hive
   */
  private def moveToHiveWarehouse(sourcePath: String, schemaName: String, tableName: String, partitionColumns: Array[Column] = Array.empty, isBackup: Boolean = false): String = {
    val hiveWarehousePath = s"$HIVE_WAREHOUSE/$schemaName.db/$tableName"

    val newPath = if (partitionColumns.nonEmpty) {
      hiveWarehousePath + "/" + preparePartitionPath(partitionColumns)
    } else {
      hiveWarehousePath
    }

    HdfsService.deleteFolder(newPath, isBackup)
    HdfsService.copyFolder(sourcePath, newPath)

    newPath
  }

  /**
   * Create Hive database
   *
   * @deprecated Do it manually
   * @param spark  Spark Session
   * @param schema Database name
   */
  def createHiveSchema(spark: SparkSession, schema: String): Unit = {
    spark.sql("create database " + schema)
  }

  /**
   * Check if Hive db already exists
   *
   * @param spark      Spark Session
   * @param schemaName Db name
   * @return true if exists, false if not exist
   */
  def checkExistHiveSchema(spark: SparkSession, schemaName: String): Boolean = {
    val databaseList = spark.sql("show databases")
      .select("namespace").distinct().collect().map(_.getString(0).toLowerCase())
    logger.info("Checking database named: " + schemaName)
    databaseList.contains(schemaName.toLowerCase())
  }

  /**
   * Check if Hive table already exists
   *
   * @param spark      Spark Session
   * @param schemaName Db name
   * @param tableName  Table name
   * @return true if exists, false if not exist
   */
  def checkExistHiveTable(spark: SparkSession, schemaName: String, tableName: String): Boolean = {
    import spark.implicits._
    logger.info("Checking tables: " + schemaName + "." + tableName)
    spark.sql("use " + schemaName)
    val tableListDs = spark.catalog.listTables(schemaName)

    val table = tableListDs.where(s"name == '$tableName'")

    var isExternalTable = false
    var isValid = false
    if (table.count() == 1) {
      val tableType = table.select("tableType").as[String].head()

      if (tableType == "EXTERNAL") {
        isExternalTable = true
      }

      val tableInfo = spark.sql(s"show create table $tableName").as[String].head()
      var isParquetInputType = false
      if (tableInfo.contains("USING parquet")) {
        isParquetInputType = true
      }

      if (isExternalTable && isParquetInputType) {
        isValid = true
      }

      if (!isExternalTable) {
        logger.error("Table type is not EXTERNAL")
        throw new Exception("Table type is not EXTERNAL")
      }

      if (!isParquetInputType) {
        logger.error("Input type is not Parquet")
        throw new Exception("Input type is not Parquet")
      }
    } else {
      logger.error(s"Table $schemaName.$tableName not exist")
    }

    isValid
  }

  /**
   * Create Hive normal table
   *
   * @param spark     Spark Session
   * @param df        Input dataframe
   * @param tableName Table name
   */
  def createHiveTable(spark: SparkSession, df: DataFrame, tableName: String): Unit = {
    logger.info("create a Hive table: " + tableName)
    val colSQLString = df.schema.fields.filter(_.name != "ds")
      .map(field => field.name + " " + field.dataType.typeName).mkString(",")

    logger.info("colSQL String: " + colSQLString)
    val createTableSQL = s"create table $tableName ($colSQLString)" +
      "PARTITIONED BY (ds string)" +
      "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'" +
      "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'" +
      "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'"

    spark.sql(createTableSQL)
    logger.info("create table SQL: " + createTableSQL)
  }

  /**
   * Create Hive external snapshot table
   *
   * @param spark      Spark Session
   * @param df         Input dataframe
   * @param schemaName Db name
   * @param tableName  Table name
   * @param inputPath  Data input path
   * @param isBackup   Need backup?
   */
  def createExternalHiveSnapshotTable(spark: SparkSession, df: DataFrame, schemaName: String, tableName: String, inputPath: String, isBackup: Boolean): Unit = {
    logger.info("Create a Hive snapshot table: " + schemaName + "." + tableName)
    val colSQLString = df.schema.fields
      .map(field => field.name + " " + field.dataType.typeName).mkString(",")
    logger.info("colSQL String: " + colSQLString)

    val hiveWarehousePath = moveToHiveWarehouse(sourcePath = inputPath,
      schemaName = schemaName, tableName = tableName, isBackup = isBackup)

    val createTableSQL = s"CREATE EXTERNAL TABLE IF NOT EXISTS $schemaName.$tableName ($colSQLString)" +
      "STORED AS PARQUET " +
      s"LOCATION '$hiveWarehousePath'"

    logger.info("Create table SQL: " + createTableSQL)
    spark.sql(createTableSQL)
  }

  /**
   * Register new location for Hive snapshot table
   *
   * @param schemaName Hive schema name
   * @param tableName  Hive table name (dbName.tableName)
   * @param inputPath  Hive new data location path
   * @param isBackup   Need backup?
   * @param spark      Spark Session
   */
  def registerSnapshotTable(schemaName: String, tableName: String, inputPath: String, isBackup: Boolean, spark: SparkSession): Unit = {
    logger.info("Register partition for table: " + tableName)

    val hiveWarehousePath = moveToHiveWarehouse(sourcePath = inputPath,
      schemaName = schemaName, tableName = tableName, isBackup = isBackup)

    logger.info("Inserting data into table | table_name = " + tableName)
    spark.sql(s"ALTER TABLE $tableName SET LOCATION '$hiveWarehousePath'")
    logger.info("Inserting data into table successful | table_name = " + tableName)
  }

  /**
   * Transform List column to create string sql
   *
   * @param columns List column
   * @return Column string sql
   */
  private def preparePartitionCreateSql(columns: Array[Column]): String = {
    val partitionColSqlString = columns.map(column => {
      s"${column.colName} ${column.colType}"
    }).mkString("(", ",", ")")
    logger.info("partitionColSql String: " + partitionColSqlString)

    partitionColSqlString
  }

  /**
   * Transform List column to path
   *
   * @param columns Array column
   * @return path
   */
  private def preparePartitionPath(columns: Array[Column]): String = {
    val partitionPathString = columns.map(column => {
      val colName = column.colName
      val colValue = column.colValue.toString

      s"$colName=$colValue"
    }).mkString("/")
    logger.info("partitionPathString String: " + partitionPathString)

    partitionPathString
  }

  /**
   * Transform List column to alter string sql
   *
   * @param columns List column
   * @param logDate Log date
   * @return Column string sql
   */
  private def preparePartitionRegisterSql(columns: Array[Column], logDate: String): String = {
    val partitionColSqlString = columns.map(column => {
      val colName = column.colName
      val colType = column.colType
      val colValue = column.colValue

      if (Array("integer", "double", "long", "decimal").contains(colType)) {
        s"$colName = $colValue"
      } else {
        s"$colName = '$colValue'"
      }

    }).mkString(",")
    logger.info("partitionColSql String: " + partitionColSqlString)

    partitionColSqlString
  }

  /**
   * Create Hive external table
   *
   * @param spark      Spark Session
   * @param df         Input dataframe
   * @param schemaName Db name
   * @param tableName  Table name
   * @param inputPath  Data input path
   * @param isBackup   Need backup?
   */
  def createExternalHiveTable(spark: SparkSession, df: DataFrame, schemaName: String, tableName: String,
                              partitionFields: Array[Column], inputPath: String, logDate: String, isBackup: Boolean): Unit = {
    logger.info("create a Hive table: " + tableName)
    val partitionFieldNameList = partitionFields.map(_.colName)
    val colSQLString = df.schema.fields.filter(field => !partitionFieldNameList.contains(field.name))
      .map(field => s"${field.name} ${field.dataType.typeName}").mkString("(", ",", ")")
    logger.info("colSQL String: " + colSQLString)

    val partitionFieldsApplied = applyPartitionColumn(partitionFields, logDate)
    val partitionColSqlString = preparePartitionCreateSql(partitionFieldsApplied)

    val hiveWarehousePath = moveToHiveWarehouse(sourcePath = inputPath,
      schemaName = schemaName, tableName = tableName, partitionColumns = partitionFieldsApplied, isBackup = isBackup)

    val createTableSQL =
      s""" CREATE EXTERNAL TABLE IF NOT EXISTS $schemaName.$tableName $colSQLString
         | PARTITIONED BY $partitionColSqlString
         | STORED AS PARQUET
         | LOCATION '$hiveWarehousePath'
         |""".stripMargin

    spark.sql(createTableSQL)
    logger.info("create table SQL: " + createTableSQL)
  }

  /**
   * Register new partition for Hive external table
   *
   * @param logDate         Report date using for partitioning
   * @param hiveSchema      Hive schema name
   * @param hiveTable       Hive table name (dbName.tableName)
   * @param inputPath       Hive new data location path
   * @param partitionFields List partition column
   * @param isBackup        Need backup?
   * @param spark           Spark Session
   */
  def registerTable(logDate: String, hiveSchema: String, hiveTable: String, inputPath: String, partitionFields: Array[Column], isBackup: Boolean, spark: SparkSession): Unit = {
    logger.info("Register partition for table: " + hiveTable)

    val partitionFieldsApplied = applyPartitionColumn(partitionFields, logDate)
    val partitionColSqlString = preparePartitionRegisterSql(partitionFieldsApplied, logDate)
    logger.info("partitionColSql String: " + partitionColSqlString)

    val dropSql = s"ALTER TABLE $hiveTable DROP IF EXISTS PARTITION ($partitionColSqlString)"
    logger.info(s"Delete data for table = $hiveTable, sql = $dropSql")
    spark.sql(dropSql)
    logger.info("Delete data successful for table_name = " + hiveTable)

    val hiveWarehousePath = moveToHiveWarehouse(sourcePath = inputPath,
      schemaName = hiveSchema, tableName = hiveTable, partitionColumns = partitionFieldsApplied, isBackup = isBackup)

    val addSql = s"ALTER TABLE $hiveTable ADD IF NOT EXISTS PARTITION ($partitionColSqlString) LOCATION '$hiveWarehousePath'"
    logger.info(s"Inserting data into table | table_name = $hiveTable, sql = $addSql")
    spark.sql(addSql)
    logger.info("Inserting data into table successful | table_name = " + hiveTable)
  }

  def cache(df: DataFrame, tableName: String): Unit = {
    df.createOrReplaceGlobalTempView(tableName)
  }

  def cacheExists(spark: SparkSession, tableName: String): Boolean = {
    spark.catalog.tableExists(s"global_temp.$tableName")
  }

  def getCache(spark: SparkSession, tableName: String): DataFrame = {
    spark.table(s"global_temp.$tableName")
  }
}
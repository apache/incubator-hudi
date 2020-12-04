/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.client.{HoodieWriteResult, SparkRDDWriteClient}
import org.apache.hudi.common.config.{HoodieConfig, HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.model.{HoodieRecordPayload, HoodieTableType, WriteOperationType}
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.util.{CommitUtils, ReflectionUtils}
import org.apache.hudi.config.HoodieBootstrapConfig.{BOOTSTRAP_BASE_PATH_PROP, BOOTSTRAP_INDEX_CLASS_PROP}
import org.apache.hudi.config.{HoodieInternalConfig, HoodieWriteConfig}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.execution.bulkinsert.BulkInsertInternalPartitionerWithRowsFactory
import org.apache.hudi.hive.{HiveSyncConfig, HiveSyncTool}
import org.apache.hudi.index.SparkHoodieIndex
import org.apache.hudi.internal.DataSourceInternalWriterHelper
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory
import org.apache.hudi.sync.common.AbstractSyncTool
import org.apache.hudi.table.BulkInsertPartitioner
import org.apache.log4j.LogManager
import org.apache.spark.{SPARK_VERSION, SparkContext}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset,Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

import java.util
import java.util.Properties
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object HoodieSparkSqlWriter {

  private val log = LogManager.getLogger(getClass)
  private var tableExists: Boolean = false
  private var asyncCompactionTriggerFnDefined: Boolean = false
  private var asyncClusteringTriggerFnDefined: Boolean = false

  def write(sqlContext: SQLContext,
            mode: SaveMode,
            parameters: Map[String, String],
            df: DataFrame,
            hoodieTableConfigOpt: Option[HoodieTableConfig] = Option.empty,
            hoodieWriteClient: Option[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]] = Option.empty,
            asyncCompactionTriggerFn: Option[Function1[SparkRDDWriteClient[HoodieRecordPayload[Nothing]], Unit]] = Option.empty,
            asyncClusteringTriggerFn: Option[Function1[SparkRDDWriteClient[HoodieRecordPayload[Nothing]], Unit]] = Option.empty
           )
  : (Boolean, common.util.Option[String], common.util.Option[String], common.util.Option[String],
    SparkRDDWriteClient[HoodieRecordPayload[Nothing]], HoodieTableConfig) = {

    val sparkContext = sqlContext.sparkContext
    val path = parameters.get("path")
    val hoodieConfig = HoodieWriterUtils.convertMapToHoodieConfig(parameters)
    val tblNameOp = hoodieConfig.getStringOrThrow(HoodieWriteConfig.TABLE_NAME, s"'${HoodieWriteConfig.TABLE_NAME.key}' must be set.")
    asyncCompactionTriggerFnDefined = asyncCompactionTriggerFn.isDefined
    asyncClusteringTriggerFnDefined = asyncClusteringTriggerFn.isDefined
    if (path.isEmpty) {
      throw new HoodieException(s"'path' must be set.")
    }
    val tblName = tblNameOp.trim
    sparkContext.getConf.getOption("spark.serializer") match {
      case Some(ser) if ser.equals("org.apache.spark.serializer.KryoSerializer") =>
      case _ => throw new HoodieException("hoodie only support org.apache.spark.serializer.KryoSerializer as spark.serializer")
    }
    val tableType = HoodieTableType.valueOf(hoodieConfig.getString(TABLE_TYPE_OPT_KEY))
    var operation = WriteOperationType.fromValue(hoodieConfig.getString(OPERATION_OPT_KEY))
    // It does not make sense to allow upsert() operation if INSERT_DROP_DUPS_OPT_KEY is true
    // Auto-correct the operation to "insert" if OPERATION_OPT_KEY is set to "upsert" wrongly
    // or not set (in which case it will be set as "upsert" by parametersWithWriteDefaults()) .
    if (hoodieConfig.getBoolean(INSERT_DROP_DUPS_OPT_KEY) &&
      operation == WriteOperationType.UPSERT) {

      log.warn(s"$UPSERT_OPERATION_OPT_VAL is not applicable " +
        s"when $INSERT_DROP_DUPS_OPT_KEY is set to be true, " +
        s"overriding the $OPERATION_OPT_KEY to be $INSERT_OPERATION_OPT_VAL")

      operation = WriteOperationType.INSERT
    }

    val jsc = new JavaSparkContext(sparkContext)
    val basePath = new Path(path.get)
    val instantTime = HoodieActiveTimeline.createNewInstantTime()
    val fs = basePath.getFileSystem(sparkContext.hadoopConfiguration)
    tableExists = fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))
    var tableConfig = getHoodieTableConfig(sparkContext, path.get, hoodieTableConfigOpt)
    val keyGenerator = HoodieSparkKeyGeneratorFactory.createKeyGenerator(toProperties(parameters))

    if (mode == SaveMode.Ignore && tableExists) {
      log.warn(s"hoodie table at $basePath already exists. Ignoring & not performing actual writes.")
      (false, common.util.Option.empty(), common.util.Option.empty(), common.util.Option.empty(), hoodieWriteClient.orNull, tableConfig)
    } else {
      // Handle various save modes
      handleSaveModes(mode, basePath, tableConfig, tblName, operation, fs)
      // Create the table if not present
      if (!tableExists) {
        val archiveLogFolder = hoodieConfig.getStringOrDefault(HoodieTableConfig.HOODIE_ARCHIVELOG_FOLDER_PROP)
        val partitionColumns = HoodieWriterUtils.getPartitionColumns(keyGenerator)

        val tableMetaClient = HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(tableType)
          .setTableName(tblName)
          .setArchiveLogFolder(archiveLogFolder)
          .setPayloadClassName(hoodieConfig.getString(PAYLOAD_CLASS_OPT_KEY))
          .setPreCombineField(hoodieConfig.getStringOrDefault(PRECOMBINE_FIELD_OPT_KEY, null))
          .setPartitionColumns(partitionColumns)
          .initTable(sparkContext.hadoopConfiguration, path.get)
        tableConfig = tableMetaClient.getTableConfig
      }

      val commitActionType = CommitUtils.getCommitActionType(operation, tableConfig.getTableType)

      // short-circuit if bulk_insert via row is enabled.
      // scalastyle:off
      if (hoodieConfig.getBoolean(ENABLE_ROW_WRITER_OPT_KEY) &&
        operation == WriteOperationType.BULK_INSERT) {
        val (success, commitTime: common.util.Option[String]) = bulkInsertAsRow(sqlContext, parameters, df, tblName,
                                                                                basePath, path, instantTime)
        return (success, commitTime, common.util.Option.empty(), common.util.Option.empty(), hoodieWriteClient.orNull, tableConfig)
      }
      // scalastyle:on

      val (writeResult, writeClient: SparkRDDWriteClient[HoodieRecordPayload[Nothing]]) =
        if (operation != WriteOperationType.DELETE) {
          // register classes & schemas
          val (structName, nameSpace) = AvroConversionUtils.getAvroRecordNameAndNamespace(tblName)
          sparkContext.getConf.registerKryoClasses(
            Array(classOf[org.apache.avro.generic.GenericData],
              classOf[org.apache.avro.Schema]))
          val schema = AvroConversionUtils.convertStructTypeToAvroSchema(df.schema, structName, nameSpace)
          sparkContext.getConf.registerAvroSchemas(schema)
          log.info(s"Registered avro schema : ${schema.toString(true)}")

          // Convert to RDD[HoodieRecord]
          val genericRecords: RDD[GenericRecord] = HoodieSparkUtils.createRdd(df, schema, structName, nameSpace)
          val shouldCombine = parameters(INSERT_DROP_DUPS_OPT_KEY.key()).toBoolean || operation.equals(WriteOperationType.UPSERT);
          val hoodieAllIncomingRecords = genericRecords.map(gr => {
            val hoodieRecord = if (shouldCombine) {
              val orderingVal = HoodieAvroUtils.getNestedFieldVal(gr, hoodieConfig.getString(PRECOMBINE_FIELD_OPT_KEY), false)
                .asInstanceOf[Comparable[_]]
              DataSourceUtils.createHoodieRecord(gr,
                orderingVal, keyGenerator.getKey(gr),
                hoodieConfig.getString(PAYLOAD_CLASS_OPT_KEY))
            } else {
              DataSourceUtils.createHoodieRecord(gr, keyGenerator.getKey(gr), hoodieConfig.getString(PAYLOAD_CLASS_OPT_KEY))
            }
            hoodieRecord
          }).toJavaRDD()

          // Create a HoodieWriteClient & issue the write.
          val client = hoodieWriteClient.getOrElse(DataSourceUtils.createHoodieClient(jsc, schema.toString, path.get,
            tblName, mapAsJavaMap(parameters - HoodieWriteConfig.HOODIE_AUTO_COMMIT_PROP.key)
          )).asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]]

          if (isAsyncCompactionEnabled(client, tableConfig, parameters, jsc.hadoopConfiguration())) {
            asyncCompactionTriggerFn.get.apply(client)
          }

          if (isAsyncClusteringEnabled(client, parameters)) {
            asyncClusteringTriggerFn.get.apply(client)
          }

          val hoodieRecords =
            if (hoodieConfig.getBoolean(INSERT_DROP_DUPS_OPT_KEY)) {
              DataSourceUtils.dropDuplicates(jsc, hoodieAllIncomingRecords, mapAsJavaMap(parameters))
            } else {
              hoodieAllIncomingRecords
            }
          client.startCommitWithTime(instantTime, commitActionType)
          val writeResult = DataSourceUtils.doWriteOperation(client, hoodieRecords, instantTime, operation)
          (writeResult, client)
        } else {
          val structName = s"${tblName}_record"
          val nameSpace = s"hoodie.${tblName}"
          sparkContext.getConf.registerKryoClasses(
            Array(classOf[org.apache.avro.generic.GenericData],
              classOf[org.apache.avro.Schema]))

          // Convert to RDD[HoodieKey]
          val genericRecords: RDD[GenericRecord] = HoodieSparkUtils.createRdd(df, structName, nameSpace)
          val hoodieKeysToDelete = genericRecords.map(gr => keyGenerator.getKey(gr)).toJavaRDD()

          if (!tableExists) {
            throw new HoodieException(s"hoodie table at $basePath does not exist")
          }

          // Create a HoodieWriteClient & issue the delete.
          val client = hoodieWriteClient.getOrElse(DataSourceUtils.createHoodieClient(jsc,
            null, path.get, tblName,
            mapAsJavaMap(parameters - HoodieWriteConfig.HOODIE_AUTO_COMMIT_PROP.key)))
            .asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]]

          if (isAsyncCompactionEnabled(client, tableConfig, parameters, jsc.hadoopConfiguration())) {
            asyncCompactionTriggerFn.get.apply(client)
          }

          if (isAsyncClusteringEnabled(client, parameters)) {
            asyncClusteringTriggerFn.get.apply(client)
          }

          // Issue deletes
          client.startCommitWithTime(instantTime, commitActionType)
          val writeStatuses = DataSourceUtils.doDeleteOperation(client, hoodieKeysToDelete, instantTime)
          (writeStatuses, client)
        }

      // Check for errors and commit the write.
      val (writeSuccessful, compactionInstant, clusteringInstant) =
        commitAndPerformPostOperations(sqlContext.sparkSession, df.schema,
          writeResult, parameters, writeClient, tableConfig, jsc,
          TableInstantInfo(basePath, instantTime, commitActionType, operation))

      def unpersistRdd(rdd: RDD[_]): Unit = {
        if (sparkContext.getPersistentRDDs.contains(rdd.id)) {
          try {
            rdd.unpersist()
          } catch {
            case t: Exception => log.warn("Got excepting trying to unpersist rdd", t)
          }
        }
        val parentRdds = rdd.dependencies.map(_.rdd)
        parentRdds.foreach { parentRdd =>
          unpersistRdd(parentRdd)
        }
      }
      // it's safe to unpersist cached rdds here
      unpersistRdd(writeResult.getWriteStatuses.rdd)

      (writeSuccessful, common.util.Option.ofNullable(instantTime), compactionInstant, clusteringInstant, writeClient, tableConfig)
    }
  }

  def bootstrap(sqlContext: SQLContext,
                mode: SaveMode,
                parameters: Map[String, String],
                df: DataFrame,
                hoodieTableConfigOpt: Option[HoodieTableConfig] = Option.empty,
                hoodieWriteClient: Option[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]] = Option.empty): Boolean = {

    val sparkContext = sqlContext.sparkContext
    val path = parameters.getOrElse("path", throw new HoodieException("'path' must be set."))
    val hoodieConfig = HoodieWriterUtils.convertMapToHoodieConfig(parameters)
    val tableName = hoodieConfig.getStringOrThrow(HoodieWriteConfig.TABLE_NAME, s"'${HoodieWriteConfig.TABLE_NAME.key}' must be set.")
    val tableType = hoodieConfig.getStringOrDefault(TABLE_TYPE_OPT_KEY)
    val bootstrapBasePath = hoodieConfig.getStringOrThrow(BOOTSTRAP_BASE_PATH_PROP,
      s"'${BOOTSTRAP_BASE_PATH_PROP.key}' is required for '${BOOTSTRAP_OPERATION_OPT_VAL}'" +
        " operation'")
    val bootstrapIndexClass = hoodieConfig.getStringOrDefault(BOOTSTRAP_INDEX_CLASS_PROP)

    var schema: String = null
    if (df.schema.nonEmpty) {
      val (structName, namespace) = AvroConversionUtils.getAvroRecordNameAndNamespace(tableName)
      schema = AvroConversionUtils.convertStructTypeToAvroSchema(df.schema, structName, namespace).toString
    } else {
      schema = HoodieAvroUtils.getNullSchema.toString
    }

    val basePath = new Path(path)
    val fs = basePath.getFileSystem(sparkContext.hadoopConfiguration)
    tableExists = fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))
    val tableConfig = getHoodieTableConfig(sparkContext, path, hoodieTableConfigOpt)

    // Handle various save modes
    if (mode == SaveMode.Ignore && tableExists) {
      log.warn(s"hoodie table at $basePath already exists. Ignoring & not performing actual writes.")
      false
    } else {
      handleSaveModes(mode, basePath, tableConfig, tableName, WriteOperationType.BOOTSTRAP, fs)
    }

    if (!tableExists) {
      val archiveLogFolder = hoodieConfig.getStringOrDefault(HoodieTableConfig.HOODIE_ARCHIVELOG_FOLDER_PROP)
      val partitionColumns = HoodieWriterUtils.getPartitionColumns(parameters)
      HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(HoodieTableType.valueOf(tableType))
          .setTableName(tableName)
          .setArchiveLogFolder(archiveLogFolder)
          .setPayloadClassName(hoodieConfig.getStringOrDefault(PAYLOAD_CLASS_OPT_KEY))
          .setPreCombineField(hoodieConfig.getStringOrDefault(PRECOMBINE_FIELD_OPT_KEY, null))
          .setBootstrapIndexClass(bootstrapIndexClass)
          .setBootstrapBasePath(bootstrapBasePath)
          .setPartitionColumns(partitionColumns)
          .initTable(sparkContext.hadoopConfiguration, path)
    }

    val jsc = new JavaSparkContext(sqlContext.sparkContext)
    val writeClient = hoodieWriteClient.getOrElse(DataSourceUtils.createHoodieClient(jsc,
      schema, path, tableName, mapAsJavaMap(parameters)))
    try {
      writeClient.bootstrap(org.apache.hudi.common.util.Option.empty())
    } finally {
      writeClient.close()
    }
    val metaSyncSuccess = metaSync(sqlContext.sparkSession, hoodieConfig, basePath, df.schema)
    metaSyncSuccess
  }

  def bulkInsertAsRow(sqlContext: SQLContext,
                      parameters: Map[String, String],
                      df: DataFrame,
                      tblName: String,
                      basePath: Path,
                      path: Option[String],
                      instantTime: String): (Boolean, common.util.Option[String]) = {
    val sparkContext = sqlContext.sparkContext
    // register classes & schemas
    val (structName, nameSpace) = AvroConversionUtils.getAvroRecordNameAndNamespace(tblName)
    sparkContext.getConf.registerKryoClasses(
      Array(classOf[org.apache.avro.generic.GenericData],
        classOf[org.apache.avro.Schema]))
    val schema = AvroConversionUtils.convertStructTypeToAvroSchema(df.schema, structName, nameSpace)
    sparkContext.getConf.registerAvroSchemas(schema)
    log.info(s"Registered avro schema : ${schema.toString(true)}")
    if (parameters(INSERT_DROP_DUPS_OPT_KEY.key).toBoolean) {
      throw new HoodieException("Dropping duplicates with bulk_insert in row writer path is not supported yet")
    }
    val params = parameters.updated(HoodieWriteConfig.AVRO_SCHEMA.key, schema.toString)
    val writeConfig = DataSourceUtils.createHoodieConfig(schema.toString, path.get, tblName, mapAsJavaMap(params))
    val userDefinedBulkInsertPartitionerOpt = DataSourceUtils.createUserDefinedBulkInsertPartitionerWithRows(writeConfig)
    val bulkInsertPartitionerRows : BulkInsertPartitioner[Dataset[Row]] = if (userDefinedBulkInsertPartitionerOpt.isPresent)  {
      userDefinedBulkInsertPartitionerOpt.get
    }
    else {
      BulkInsertInternalPartitionerWithRowsFactory.get(writeConfig.getBulkInsertSortMode)
    }
    val arePartitionRecordsSorted = bulkInsertPartitionerRows.arePartitionRecordsSorted();
    parameters.updated(HoodieInternalConfig.BULKINSERT_ARE_PARTITIONER_RECORDS_SORTED, arePartitionRecordsSorted.toString)
    val isGlobalIndex = SparkHoodieIndex.isGlobalIndex(writeConfig)
    val hoodieDF = HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsert(sqlContext, writeConfig, df, structName, nameSpace,
      bulkInsertPartitionerRows, isGlobalIndex)
    if (SPARK_VERSION.startsWith("2.")) {
      hoodieDF.write.format("org.apache.hudi.internal")
        .option(DataSourceInternalWriterHelper.INSTANT_TIME_OPT_KEY, instantTime)
        .options(params)
        .save()
    } else if (SPARK_VERSION.startsWith("3.")) {
      hoodieDF.write.format("org.apache.hudi.spark3.internal")
        .option(DataSourceInternalWriterHelper.INSTANT_TIME_OPT_KEY, instantTime)
        .option(HoodieInternalConfig.BULKINSERT_INPUT_DATA_SCHEMA_DDL.key, hoodieDF.schema.toDDL)
        .options(params)
        .mode(SaveMode.Append)
        .save()
    } else {
      throw new HoodieException("Bulk insert using row writer is not supported with current Spark version."
        + " To use row writer please switch to spark 2 or spark 3")
    }
    val hoodieConfig = HoodieWriterUtils.convertMapToHoodieConfig(params)
    val hiveSyncEnabled = hoodieConfig.getStringOrDefault(HIVE_SYNC_ENABLED_OPT_KEY).toBoolean
    val metaSyncEnabled = hoodieConfig.getStringOrDefault(META_SYNC_ENABLED_OPT_KEY).toBoolean
    val syncHiveSuccess =
      if (hiveSyncEnabled || metaSyncEnabled) {
        metaSync(sqlContext.sparkSession, hoodieConfig, basePath, df.schema)
    } else {
      true
    }
    (syncHiveSuccess, common.util.Option.ofNullable(instantTime))
  }

  def toProperties(params: Map[String, String]): TypedProperties = {
    val props = new TypedProperties()
    params.foreach(kv => props.setProperty(kv._1, kv._2))
    props
  }

  private def handleSaveModes(mode: SaveMode, tablePath: Path, tableConfig: HoodieTableConfig, tableName: String,
                              operation: WriteOperationType, fs: FileSystem): Unit = {
    if (mode == SaveMode.Append && tableExists) {
      val existingTableName = tableConfig.getTableName
      if (!existingTableName.equals(tableName)) {
        throw new HoodieException(s"hoodie table with name $existingTableName already exists at $tablePath")
      }
    }

    if (operation != WriteOperationType.DELETE) {
      if (mode == SaveMode.ErrorIfExists && tableExists) {
        throw new HoodieException(s"hoodie table at $tablePath already exists.")
      } else if (mode == SaveMode.Overwrite && tableExists && operation !=  WriteOperationType.INSERT_OVERWRITE_TABLE) {
        // When user set operation as INSERT_OVERWRITE_TABLE,
        // overwrite will use INSERT_OVERWRITE_TABLE operator in doWriteOperation
        log.warn(s"hoodie table at $tablePath already exists. Deleting existing data & overwriting with new data.")
        fs.delete(tablePath, true)
        tableExists = false
      }
    } else {
      // Delete Operation only supports Append mode
      if (mode != SaveMode.Append) {
        throw new HoodieException(s"Append is the only save mode applicable for ${operation.toString} operation")
      }
    }
  }

  private def syncHive(basePath: Path, fs: FileSystem, hoodieConfig: HoodieConfig, sqlConf: SQLConf): Boolean = {
    val hiveSyncConfig: HiveSyncConfig = buildSyncConfig(basePath, hoodieConfig, sqlConf)
    val hiveConf: HiveConf = new HiveConf()
    hiveConf.addResource(fs.getConf)
    new HiveSyncTool(hiveSyncConfig, hiveConf, fs).syncHoodieTable()
    true
  }

  private def buildSyncConfig(basePath: Path, hoodieConfig: HoodieConfig, sqlConf: SQLConf): HiveSyncConfig = {
    val hiveSyncConfig: HiveSyncConfig = new HiveSyncConfig()
    hiveSyncConfig.basePath = basePath.toString
    hiveSyncConfig.baseFileFormat = hoodieConfig.getString(HIVE_BASE_FILE_FORMAT_OPT_KEY)
    hiveSyncConfig.usePreApacheInputFormat =
      hoodieConfig.getStringOrDefault(HIVE_USE_PRE_APACHE_INPUT_FORMAT_OPT_KEY).toBoolean
    hiveSyncConfig.databaseName = hoodieConfig.getString(HIVE_DATABASE_OPT_KEY)
    hiveSyncConfig.tableName = hoodieConfig.getString(HIVE_TABLE_OPT_KEY)
    hiveSyncConfig.hiveUser = hoodieConfig.getString(HIVE_USER_OPT_KEY)
    hiveSyncConfig.hivePass = hoodieConfig.getString(HIVE_PASS_OPT_KEY)
    hiveSyncConfig.jdbcUrl = hoodieConfig.getString(HIVE_URL_OPT_KEY)
    hiveSyncConfig.skipROSuffix = hoodieConfig.getStringOrDefault(HIVE_SKIP_RO_SUFFIX,
      DataSourceWriteOptions.HIVE_SKIP_RO_SUFFIX.defaultValue).toBoolean
    hiveSyncConfig.partitionFields =
      ListBuffer(hoodieConfig.getString(HIVE_PARTITION_FIELDS_OPT_KEY).split(",").map(_.trim).filter(!_.isEmpty).toList: _*)
    hiveSyncConfig.partitionValueExtractorClass = hoodieConfig.getString(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY)
    hiveSyncConfig.useJdbc = hoodieConfig.getBoolean(HIVE_USE_JDBC_OPT_KEY)
    hiveSyncConfig.useFileListingFromMetadata = hoodieConfig.getBoolean(HoodieMetadataConfig.METADATA_ENABLE_PROP)
    hiveSyncConfig.verifyMetadataFileListing = hoodieConfig.getBoolean(HoodieMetadataConfig.METADATA_VALIDATE_PROP)
    hiveSyncConfig.ignoreExceptions = hoodieConfig.getStringOrDefault(HIVE_IGNORE_EXCEPTIONS_OPT_KEY).toBoolean
    hiveSyncConfig.supportTimestamp = hoodieConfig.getStringOrDefault(HIVE_SUPPORT_TIMESTAMP).toBoolean
    hiveSyncConfig.autoCreateDatabase = hoodieConfig.getStringOrDefault(HIVE_AUTO_CREATE_DATABASE_OPT_KEY).toBoolean
    hiveSyncConfig.decodePartition = hoodieConfig.getStringOrDefault(URL_ENCODE_PARTITIONING_OPT_KEY).toBoolean
    hiveSyncConfig.batchSyncNum = hoodieConfig.getStringOrDefault(HIVE_BATCH_SYNC_PARTITION_NUM).toInt

    hiveSyncConfig.syncAsSparkDataSourceTable =  hoodieConfig.getStringOrDefault(HIVE_SYNC_AS_DATA_SOURCE_TABLE).toBoolean
    hiveSyncConfig.sparkSchemaLengthThreshold = sqlConf.getConf(StaticSQLConf.SCHEMA_STRING_LENGTH_THRESHOLD)
    hiveSyncConfig.createManagedTable = hoodieConfig.getBoolean(HIVE_CREATE_MANAGED_TABLE)
    hiveSyncConfig
  }

  private def metaSync(spark: SparkSession, hoodieConfig: HoodieConfig, basePath: Path,
                       schema: StructType): Boolean = {
    val hiveSyncEnabled = hoodieConfig.getStringOrDefault(HIVE_SYNC_ENABLED_OPT_KEY).toBoolean
    var metaSyncEnabled = hoodieConfig.getStringOrDefault(META_SYNC_ENABLED_OPT_KEY).toBoolean
    var syncClientToolClassSet = scala.collection.mutable.Set[String]()
    hoodieConfig.getString(META_SYNC_CLIENT_TOOL_CLASS).split(",").foreach(syncClass =>  syncClientToolClassSet += syncClass)

    // for backward compatibility
    if (hiveSyncEnabled) {
      metaSyncEnabled = true
      syncClientToolClassSet += classOf[HiveSyncTool].getName
    }
    var metaSyncSuccess = true
    if (metaSyncEnabled) {
      val fs = basePath.getFileSystem(spark.sessionState.newHadoopConf())
      syncClientToolClassSet.foreach(impl => {
        val syncSuccess = impl.trim match {
          case "org.apache.hudi.hive.HiveSyncTool" => {
            log.info("Syncing to Hive Metastore (URL: " + hoodieConfig.getString(HIVE_URL_OPT_KEY) + ")")
            syncHive(basePath, fs, hoodieConfig, spark.sessionState.conf)
            true
          }
          case _ => {
            val properties = new Properties()
            properties.putAll(hoodieConfig.getProps)
            properties.put("basePath", basePath.toString)
            val syncHoodie = ReflectionUtils.loadClass(impl.trim, Array[Class[_]](classOf[Properties], classOf[FileSystem]), properties, fs).asInstanceOf[AbstractSyncTool]
            syncHoodie.syncHoodieTable()
            true
          }
        }
        metaSyncSuccess = metaSyncSuccess && syncSuccess
      })
    }
    metaSyncSuccess
  }

  /**
   * Group all table/action specific information into a case class.
   */
  case class TableInstantInfo(basePath: Path, instantTime: String, commitActionType: String, operation: WriteOperationType)

  private def commitAndPerformPostOperations(spark: SparkSession,
                                             schema: StructType,
                                             writeResult: HoodieWriteResult,
                                             parameters: Map[String, String],
                                             client: SparkRDDWriteClient[HoodieRecordPayload[Nothing]],
                                             tableConfig: HoodieTableConfig,
                                             jsc: JavaSparkContext,
                                             tableInstantInfo: TableInstantInfo
                                             ): (Boolean, common.util.Option[java.lang.String], common.util.Option[java.lang.String]) = {
    if(writeResult.getWriteStatuses.rdd.filter(ws => ws.hasErrors).isEmpty()) {
      log.info("Proceeding to commit the write.")
      val metaMap = parameters.filter(kv =>
        kv._1.startsWith(parameters(COMMIT_METADATA_KEYPREFIX_OPT_KEY.key)))
      val commitSuccess =
        client.commit(tableInstantInfo.instantTime, writeResult.getWriteStatuses,
          common.util.Option.of(new util.HashMap[String, String](mapAsJavaMap(metaMap))),
          tableInstantInfo.commitActionType,
          writeResult.getPartitionToReplaceFileIds)

      if (commitSuccess) {
        log.info("Commit " + tableInstantInfo.instantTime + " successful!")
      }
      else {
        log.info("Commit " + tableInstantInfo.instantTime + " failed!")
      }

      val asyncCompactionEnabled = isAsyncCompactionEnabled(client, tableConfig, parameters, jsc.hadoopConfiguration())
      val compactionInstant: common.util.Option[java.lang.String] =
        if (asyncCompactionEnabled) {
          client.scheduleCompaction(common.util.Option.of(new util.HashMap[String, String](mapAsJavaMap(metaMap))))
        } else {
          common.util.Option.empty()
        }

      log.info(s"Compaction Scheduled is $compactionInstant")

      val asyncClusteringEnabled = isAsyncClusteringEnabled(client, parameters)
      val clusteringInstant: common.util.Option[java.lang.String] =
        if (asyncClusteringEnabled) {
          client.scheduleClustering(common.util.Option.of(new util.HashMap[String, String](mapAsJavaMap(metaMap))))
        } else {
          common.util.Option.empty()
        }

      log.info(s"Clustering Scheduled is $clusteringInstant")

      val metaSyncSuccess = metaSync(spark, HoodieWriterUtils.convertMapToHoodieConfig(parameters),
        tableInstantInfo.basePath, schema)

      log.info(s"Is Async Compaction Enabled ? $asyncCompactionEnabled")
      if (!asyncCompactionEnabled && !asyncClusteringEnabled) {
        client.close()
      }
      (commitSuccess && metaSyncSuccess, compactionInstant, clusteringInstant)
    } else {
      log.error(s"${tableInstantInfo.operation} failed with errors")
      if (log.isTraceEnabled) {
        log.trace("Printing out the top 100 errors")
        writeResult.getWriteStatuses.rdd.filter(ws => ws.hasErrors)
          .take(100)
          .foreach(ws => {
            log.trace("Global error :", ws.getGlobalError)
            if (ws.getErrors.size() > 0) {
              ws.getErrors.foreach(kt =>
                log.trace(s"Error for key: ${kt._1}", kt._2))
            }
          })
      }
      (false, common.util.Option.empty(), common.util.Option.empty())
    }
  }

  private def isAsyncCompactionEnabled(client: SparkRDDWriteClient[HoodieRecordPayload[Nothing]],
                                       tableConfig: HoodieTableConfig,
                                       parameters: Map[String, String], configuration: Configuration) : Boolean = {
    log.info(s"Config.inlineCompactionEnabled ? ${client.getConfig.inlineCompactionEnabled}")
    if (asyncCompactionTriggerFnDefined && !client.getConfig.inlineCompactionEnabled
      && parameters.get(ASYNC_COMPACT_ENABLE_OPT_KEY.key).exists(r => r.toBoolean)) {
      tableConfig.getTableType == HoodieTableType.MERGE_ON_READ
    } else {
      false
    }
  }

  private def isAsyncClusteringEnabled(client: SparkRDDWriteClient[HoodieRecordPayload[Nothing]],
                                       parameters: Map[String, String]) : Boolean = {
    log.info(s"Config.asyncClusteringEnabled ? ${client.getConfig.isAsyncClusteringEnabled}")
    asyncClusteringTriggerFnDefined && client.getConfig.isAsyncClusteringEnabled &&
      parameters.get(ASYNC_CLUSTERING_ENABLE_OPT_KEY.key).exists(r => r.toBoolean)
  }

  private def getHoodieTableConfig(sparkContext: SparkContext,
                                   tablePath: String,
                                   hoodieTableConfigOpt: Option[HoodieTableConfig]): HoodieTableConfig = {
    if (tableExists) {
      hoodieTableConfigOpt.getOrElse(
        HoodieTableMetaClient.builder().setConf(sparkContext.hadoopConfiguration).setBasePath(tablePath)
          .build().getTableConfig)
    } else {
      null
    }
  }
}

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

import org.apache.hudi.common.config.ConfigProperty
import org.apache.hudi.common.fs.ConsistencyGuardConfig
import org.apache.hudi.common.model.{HoodieTableType, WriteOperationType , HoodieFileFormat}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.{HiveSyncTool, SlashEncodedDayPartitionValueExtractor}
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.keygen.{CustomKeyGenerator, SimpleKeyGenerator}
import org.apache.log4j.LogManager
import org.apache.spark.sql.execution.datasources.{DataSourceUtils => SparkDataSourceUtils}

/**
 * List of options that can be passed to the Hoodie datasource,
 * in addition to the hoodie client configs
 */

/**
  * Options supported for reading hoodie tables.
  */
object DataSourceReadOptions {

  private val log = LogManager.getLogger(DataSourceReadOptions.getClass)

  val QUERY_TYPE_SNAPSHOT_OPT_VAL = "snapshot"
  val QUERY_TYPE_READ_OPTIMIZED_OPT_VAL = "read_optimized"
  val QUERY_TYPE_INCREMENTAL_OPT_VAL = "incremental"
  val QUERY_TYPE_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.query.type")
    .defaultValue(QUERY_TYPE_SNAPSHOT_OPT_VAL)
    .withAlternatives("hoodie.datasource.view.type")
    .withDocumentation("Whether data needs to be read, in incremental mode (new data since an instantTime) " +
      "(or) Read Optimized mode (obtain latest view, based on base files) (or) Snapshot mode " +
      "(obtain latest view, by merging base and (if any) log files)")

  val REALTIME_SKIP_MERGE_OPT_VAL = "skip_merge"
  val REALTIME_PAYLOAD_COMBINE_OPT_VAL = "payload_combine"
  val REALTIME_MERGE_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.merge.type")
    .defaultValue(REALTIME_PAYLOAD_COMBINE_OPT_VAL)
    .withDocumentation("For Snapshot query on merge on read table, control whether we invoke the record " +
      s"payload implementation to merge (${REALTIME_PAYLOAD_COMBINE_OPT_VAL}) or skip merging altogether" +
      s"${REALTIME_SKIP_MERGE_OPT_VAL}")

  val READ_PATHS_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.read.paths")
    .noDefaultValue()
    .withDocumentation("Comma separated list of file paths to read within a Hudi table.")

  val READ_PRE_COMBINE_FIELD = HoodieWriteConfig.PRECOMBINE_FIELD_PROP

  val ENABLE_HOODIE_FILE_INDEX: ConfigProperty[Boolean] = ConfigProperty
    .key("hoodie.file.index.enable")
    .defaultValue(true)
    .withDocumentation("Enables use of the spark file index implementation for Hudi, "
      + "that speeds up listing of large tables.")

  val  HOODIE_BASE_FILE_FORMAT_PROP = HoodieTableConfig.HOODIE_BASE_FILE_FORMAT_PROP

  @Deprecated
  val VIEW_TYPE_OPT_KEY = "hoodie.datasource.view.type"
  @Deprecated
  val VIEW_TYPE_READ_OPTIMIZED_OPT_VAL = "read_optimized"
  @Deprecated
  val VIEW_TYPE_INCREMENTAL_OPT_VAL = "incremental"
  @Deprecated
  val VIEW_TYPE_REALTIME_OPT_VAL = "realtime"
  @Deprecated
  val DEFAULT_VIEW_TYPE_OPT_VAL = VIEW_TYPE_READ_OPTIMIZED_OPT_VAL

  val BEGIN_INSTANTTIME_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.read.begin.instanttime")
    .noDefaultValue()
    .withDocumentation("Instant time to start incrementally pulling data from. The instanttime here need not necessarily " +
      "correspond to an instant on the timeline. New data written with an instant_time > BEGIN_INSTANTTIME are fetched out. " +
      "For e.g: ‘20170901080000’ will get all new data written after Sep 1, 2017 08:00AM.")

  val END_INSTANTTIME_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.read.end.instanttime")
    .noDefaultValue()
    .withDocumentation("Instant time to limit incrementally fetched data to. " +
      "New data written with an instant_time <= END_INSTANTTIME are fetched out.")

  val INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.read.schema.use.end.instanttime")
    .defaultValue("false")
    .withDocumentation("Uses end instant schema when incrementally fetched data to. Default: users latest instant schema.")

  val PUSH_DOWN_INCR_FILTERS_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.read.incr.filters")
    .defaultValue("")
    .withDocumentation("For use-cases like DeltaStreamer which reads from Hoodie Incremental table and applies "
      + "opaque map functions, filters appearing late in the sequence of transformations cannot be automatically "
      + "pushed down. This option allows setting filters directly on Hoodie Source.")

  val INCR_PATH_GLOB_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.read.incr.path.glob")
    .defaultValue("")
    .withDocumentation("For the use-cases like users only want to incremental pull from certain partitions "
      + "instead of the full table. This option allows using glob pattern to directly filter on path.")
}

/**
  * Options supported for writing hoodie tables.
  */
object DataSourceWriteOptions {

  private val log = LogManager.getLogger(DataSourceWriteOptions.getClass)

  val BULK_INSERT_OPERATION_OPT_VAL = WriteOperationType.BULK_INSERT.value
  val INSERT_OPERATION_OPT_VAL = WriteOperationType.INSERT.value
  val UPSERT_OPERATION_OPT_VAL = WriteOperationType.UPSERT.value
  val DELETE_OPERATION_OPT_VAL = WriteOperationType.DELETE.value
  val BOOTSTRAP_OPERATION_OPT_VAL = WriteOperationType.BOOTSTRAP.value
  val INSERT_OVERWRITE_OPERATION_OPT_VAL = WriteOperationType.INSERT_OVERWRITE.value
  val INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL = WriteOperationType.INSERT_OVERWRITE_TABLE.value
  val OPERATION_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.operation")
    .defaultValue(UPSERT_OPERATION_OPT_VAL)
    .withDocumentation("Whether to do upsert, insert or bulkinsert for the write operation. " +
      "Use bulkinsert to load new data into a table, and there on use upsert/insert. " +
      "bulk insert uses a disk based write path to scale to load large inputs without need to cache it.")

  val COW_TABLE_TYPE_OPT_VAL = HoodieTableType.COPY_ON_WRITE.name
  val MOR_TABLE_TYPE_OPT_VAL = HoodieTableType.MERGE_ON_READ.name
  val TABLE_TYPE_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.table.type")
    .defaultValue(COW_TABLE_TYPE_OPT_VAL)
    .withAlternatives("hoodie.datasource.write.storage.type")
    .withDocumentation("The table type for the underlying data, for this write. This can’t change between writes.")

  @Deprecated
  val STORAGE_TYPE_OPT_KEY = "hoodie.datasource.write.storage.type"
  @Deprecated
  val COW_STORAGE_TYPE_OPT_VAL = HoodieTableType.COPY_ON_WRITE.name
  @Deprecated
  val MOR_STORAGE_TYPE_OPT_VAL = HoodieTableType.MERGE_ON_READ.name
  @Deprecated
  val DEFAULT_STORAGE_TYPE_OPT_VAL = COW_STORAGE_TYPE_OPT_VAL

  val  HOODIE_BASE_FILE_FORMAT_PROP = HoodieTableConfig.HOODIE_BASE_FILE_FORMAT_PROP


  /**
    * Translate spark parameters to hudi parameters
    *
    * @param optParams Parameters to be translated
    * @return Parameters after translation
    */
  def translateSqlOptions(optParams: Map[String, String]): Map[String, String] = {
    var translatedOptParams = optParams
    // translate the api partitionBy of spark DataFrameWriter to PARTITIONPATH_FIELD_OPT_KEY
    if (optParams.contains(SparkDataSourceUtils.PARTITIONING_COLUMNS_KEY)) {
      val partitionColumns = optParams.get(SparkDataSourceUtils.PARTITIONING_COLUMNS_KEY)
        .map(SparkDataSourceUtils.decodePartitioningColumns)
        .getOrElse(Nil)
      val keyGeneratorClass = optParams.getOrElse(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY.key(),
        DataSourceWriteOptions.DEFAULT_KEYGENERATOR_CLASS_OPT_VAL)

      val partitionPathField =
        keyGeneratorClass match {
          // Only CustomKeyGenerator needs special treatment, because it needs to be specified in a way
          // such as "field1:PartitionKeyType1,field2:PartitionKeyType2".
          // partitionBy can specify the partition like this: partitionBy("p1", "p2:SIMPLE", "p3:TIMESTAMP")
          case c if c == classOf[CustomKeyGenerator].getName =>
            partitionColumns.map(e => {
              if (e.contains(":")) {
                e
              } else {
                s"$e:SIMPLE"
              }
            }).mkString(",")
          case _ =>
            partitionColumns.mkString(",")
        }
      translatedOptParams = optParams ++ Map(PARTITIONPATH_FIELD_OPT_KEY.key -> partitionPathField)
    }
    translatedOptParams
  }

  val TABLE_NAME_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.table.name")
    .noDefaultValue()
    .withDocumentation("Table name for the datasource write. Also used to register the table into meta stores.")

  /**
    * Field used in preCombining before actual write. When two records have the same
    * key value, we will pick the one with the largest value for the precombine field,
    * determined by Object.compareTo(..)
    */
  val PRECOMBINE_FIELD_OPT_KEY = HoodieWriteConfig.PRECOMBINE_FIELD_PROP

  /**
    * Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting.
    * This will render any value set for `PRECOMBINE_FIELD_OPT_VAL` in-effective
    */
  val PAYLOAD_CLASS_OPT_KEY = HoodieWriteConfig.WRITE_PAYLOAD_CLASS

  /**
    * Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value
    * will be obtained by invoking .toString() on the field value. Nested fields can be specified using
    * the dot notation eg: `a.b.c`
    *
    */
  val RECORDKEY_FIELD_OPT_KEY = KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY

  /**
    * Partition path field. Value to be used at the `partitionPath` component of `HoodieKey`. Actual
    * value obtained by invoking .toString()
    */
  val PARTITIONPATH_FIELD_OPT_KEY = KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY

  /**
    * Flag to indicate whether to use Hive style partitioning.
    * If set true, the names of partition folders follow <partition_column_name>=<partition_value> format.
    * By default false (the names of partition folders are only partition values)
    */
  val HIVE_STYLE_PARTITIONING_OPT_KEY = KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_OPT_KEY
  val URL_ENCODE_PARTITIONING_OPT_KEY = KeyGeneratorOptions.URL_ENCODE_PARTITIONING_OPT_KEY

  /**
    * Key generator class, that implements will extract the key out of incoming record
    *
    */
  val KEYGENERATOR_CLASS_OPT_KEY = HoodieWriteConfig.KEYGENERATOR_CLASS_PROP
  val DEFAULT_KEYGENERATOR_CLASS_OPT_VAL = classOf[SimpleKeyGenerator].getName

  /**
   *
   * By default, false (will be enabled as default in a future release)
   */
  val ENABLE_ROW_WRITER_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.row.writer.enable")
    .defaultValue("false")
    .withDocumentation("When set to true, will perform write operations directly using the spark native " +
      "`Row` representation, avoiding any additional conversion costs.")

  val COMMIT_METADATA_KEYPREFIX_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.commitmeta.key.prefix")
    .defaultValue("_")
    .withDocumentation("Option keys beginning with this prefix, are automatically added to the commit/deltacommit metadata. " +
      "This is useful to store checkpointing information, in a consistent way with the hudi timeline")

  val INSERT_DROP_DUPS_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.insert.drop.duplicates")
    .defaultValue("false")
    .withDocumentation("If set to true, filters out all duplicate records from incoming dataframe, during insert operations.")

  val STREAMING_RETRY_CNT_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.streaming.retry.count")
    .defaultValue("3")
    .withDocumentation("Config to indicate how many times streaming job should retry for a failed micro batch.")

  val STREAMING_RETRY_INTERVAL_MS_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.streaming.retry.interval.ms")
    .defaultValue("2000")
    .withDocumentation(" Config to indicate how long (by millisecond) before a retry should issued for failed microbatch")

  /**
   *
   * By default true (in favor of streaming progressing over data integrity)
   */
  val STREAMING_IGNORE_FAILED_BATCH_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.streaming.ignore.failed.batch")
    .defaultValue("true")
    .withDocumentation("Config to indicate whether to ignore any non exception error (e.g. writestatus error)"
      + " within a streaming microbatch")

  val META_SYNC_CLIENT_TOOL_CLASS: ConfigProperty[String] = ConfigProperty
    .key("hoodie.meta.sync.client.tool.class")
    .defaultValue(classOf[HiveSyncTool].getName)
    .withDocumentation("Sync tool class name used to sync to metastore. Defaults to Hive.")

  // HIVE SYNC SPECIFIC CONFIGS
  // NOTE: DO NOT USE uppercase for the keys as they are internally lower-cased. Using upper-cases causes
  // unexpected issues with config getting reset
  val HIVE_SYNC_ENABLED_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.enable")
    .defaultValue("false")
    .withDocumentation("When set to true, register/sync the table to Apache Hive metastore")

  val META_SYNC_ENABLED_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.meta.sync.enable")
    .defaultValue("false")
    .withDocumentation("")

  val HIVE_DATABASE_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.database")
    .defaultValue("default")
    .withDocumentation("database to sync to")

  val HIVE_TABLE_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.table")
    .defaultValue("unknown")
    .withDocumentation("table to sync to")

  val HIVE_BASE_FILE_FORMAT_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.base_file_format")
    .defaultValue("PARQUET")
    .withDocumentation("Base file format for the sync.")

  val HIVE_USER_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.username")
    .defaultValue("hive")
    .withDocumentation("hive user name to use")

  val HIVE_PASS_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.password")
    .defaultValue("hive")
    .withDocumentation("hive password to use")

  val HIVE_URL_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.jdbcurl")
    .defaultValue("jdbc:hive2://localhost:10000")
    .withDocumentation("Hive metastore url")

  val HIVE_PARTITION_FIELDS_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.partition_fields")
    .defaultValue("")
    .withDocumentation("field in the table to use for determining hive partition columns.")

  val HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.partition_extractor_class")
    .defaultValue(classOf[SlashEncodedDayPartitionValueExtractor].getCanonicalName)
    .withDocumentation("")

  val HIVE_ASSUME_DATE_PARTITION_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.assume_date_partitioning")
    .defaultValue("false")
    .withDocumentation("Assume partitioning is yyyy/mm/dd")

  val HIVE_USE_PRE_APACHE_INPUT_FORMAT_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.use_pre_apache_input_format")
    .defaultValue("false")
    .withDocumentation("")

  val HIVE_USE_JDBC_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.use_jdbc")
    .defaultValue("true")
    .withDocumentation("Use JDBC when hive synchronization is enabled")

  val HIVE_AUTO_CREATE_DATABASE_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.auto_create_database")
    .defaultValue("true")
    .withDocumentation("Auto create hive database if does not exists")

  val HIVE_IGNORE_EXCEPTIONS_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.ignore_exceptions")
    .defaultValue("false")
    .withDocumentation("")

  val HIVE_SKIP_RO_SUFFIX: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.skip_ro_suffix")
    .defaultValue("false")
    .withDocumentation("Skip the _ro suffix for Read optimized table, when registering")

  val HIVE_SUPPORT_TIMESTAMP: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.support_timestamp")
    .defaultValue("false")
    .withDocumentation("‘INT64’ with original type TIMESTAMP_MICROS is converted to hive ‘timestamp’ type. " +
      "Disabled by default for backward compatibility.")

  val HIVE_TABLE_PROPERTIES: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.table_properties")
    .noDefaultValue()
    .withDocumentation("Additional properties to store with table.")

  val HIVE_TABLE_SERDE_PROPERTIES: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.serde_properties")
    .noDefaultValue()
    .withDocumentation("")

  val HIVE_SYNC_AS_DATA_SOURCE_TABLE: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.sync_as_datasource")
    .defaultValue("true")
    .withDocumentation("")

  // Create table as managed table
  val HIVE_CREATE_MANAGED_TABLE: ConfigProperty[Boolean] = ConfigProperty
    .key("hoodie.datasource.hive_sync.create_managed_table")
    .defaultValue(false)
    .withDocumentation("Whether to sync the table as managed table.")

  val HIVE_BATCH_SYNC_PARTITION_NUM: ConfigProperty[Int] = ConfigProperty
    .key("hoodie.datasource.hive_sync.batch_num")
    .defaultValue(1000)
    .withDocumentation("The number of partitions one batch when synchronous partitions to hive.")

  // Async Compaction - Enabled by default for MOR
  val ASYNC_COMPACT_ENABLE_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.compaction.async.enable")
    .defaultValue("true")
    .withDocumentation("Controls whether async compaction should be turned on for MOR table writing.")

  val INLINE_CLUSTERING_ENABLE_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.clustering.inline.enable")
    .defaultValue("false")
    .sinceVersion("0.9.0")
    .withDocumentation("Enable inline clustering. Disabled by default.")

  val ASYNC_CLUSTERING_ENABLE_OPT_KEY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.clustering.async.enable")
    .defaultValue("false")
    .sinceVersion("0.9.0")
    .withDocumentation("Enable asynchronous clustering. Disabled by default.")

  val KAFKA_AVRO_VALUE_DESERIALIZER_CLASS: ConfigProperty[String] = ConfigProperty
    .key("hoodie.deltastreamer.source.kafka.value.deserializer.class")
    .defaultValue("io.confluent.kafka.serializers.KafkaAvroDeserializer")
    .sinceVersion("0.9.0")
    .withDocumentation("This class is used by kafka client to deserialize the records")
}

object DataSourceOptionsHelper {

  private val log = LogManager.getLogger(DataSourceOptionsHelper.getClass)

  // put all the configs with alternatives here
  val allConfigsWithAlternatives = List(
    DataSourceReadOptions.QUERY_TYPE_OPT_KEY,
    DataSourceWriteOptions.TABLE_TYPE_OPT_KEY,
    HoodieTableConfig.HOODIE_BASE_FILE_FORMAT_PROP,
    HoodieTableConfig.HOODIE_LOG_FILE_FORMAT_PROP
  )

  // put all the deprecated configs here
  val allDeprecatedConfigs: Set[String] = Set(
    ConsistencyGuardConfig.CONSISTENCY_CHECK_ENABLED_PROP.key
  )

  // maps the deprecated config name to its latest name
  val allAlternatives: Map[String, String] = {
    val alterMap = scala.collection.mutable.Map[String, String]()
    allConfigsWithAlternatives.foreach(cfg => cfg.getAlternatives.foreach(alternative => alterMap(alternative) = cfg.key))
    alterMap.toMap
  }

  val viewTypeValueMap: Map[String, String] = Map(
    DataSourceReadOptions.VIEW_TYPE_READ_OPTIMIZED_OPT_VAL -> DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL,
    DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
    DataSourceReadOptions.VIEW_TYPE_REALTIME_OPT_VAL -> DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)

  def translateConfigurations(optParams: Map[String, String]): Map[String, String] = {
    val translatedOpt = scala.collection.mutable.Map[String, String]() ++= optParams
    optParams.keySet.foreach(opt => {
      if (allAlternatives.contains(opt) && !optParams.contains(allAlternatives(opt))) {
        log.warn(opt + " is deprecated and will be removed in a later release; Please use " + allAlternatives(opt))
        if (opt == DataSourceReadOptions.VIEW_TYPE_OPT_KEY) {
          // special handle for VIEW_TYPE_OPT_KEY, also need to translate its values
          translatedOpt ++= Map(allAlternatives(opt) -> viewTypeValueMap(optParams(opt)))
        } else {
          translatedOpt ++= Map(allAlternatives(opt) -> optParams(opt))
        }
      }
      if (allDeprecatedConfigs.contains(opt)) {
        log.warn(opt + " is deprecated and should never be used anymore")
      }
    })
    translatedOpt.toMap
  }
}

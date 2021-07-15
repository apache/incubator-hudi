/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.config;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.bootstrap.BootstrapMode;
import org.apache.hudi.client.transaction.ConflictResolutionStrategy;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.execution.bulkinsert.BulkInsertSortMode;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.metrics.datadog.DatadogHttpClient.ApiSite;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;
import org.apache.hudi.table.action.compact.strategy.CompactionStrategy;

import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.orc.CompressionKind;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Class storing configs for the HoodieWriteClient.
 */
@Immutable
public class HoodieWriteConfig extends HoodieConfig {

  private static final long serialVersionUID = 0L;

  public static final ConfigProperty<String> TABLE_NAME = ConfigProperty
      .key("hoodie.table.name")
      .noDefaultValue()
      .withDocumentation("Table name that will be used for registering with metastores like HMS. Needs to be same across runs.");

  public static final ConfigProperty<String> PRECOMBINE_FIELD_PROP = ConfigProperty
      .key("hoodie.datasource.write.precombine.field")
      .defaultValue("ts")
      .withDocumentation("Field used in preCombining before actual write. When two records have the same key value, "
          + "we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)");

  public static final ConfigProperty<String> WRITE_PAYLOAD_CLASS = ConfigProperty
      .key("hoodie.datasource.write.payload.class")
      .defaultValue(OverwriteWithLatestAvroPayload.class.getName())
      .withDocumentation("Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting. "
          + "This will render any value set for PRECOMBINE_FIELD_OPT_VAL in-effective");

  public static final ConfigProperty<String> KEYGENERATOR_CLASS_PROP = ConfigProperty
      .key("hoodie.datasource.write.keygenerator.class")
      .noDefaultValue()
      .withDocumentation("Key generator class, that implements `org.apache.hudi.keygen.KeyGenerator` "
          + "extract a key out of incoming records.");

  public static final ConfigProperty<String> KEYGENERATOR_TYPE_PROP = ConfigProperty
      .key("hoodie.datasource.write.keygenerator.type")
      .defaultValue(KeyGeneratorType.SIMPLE.name())
      .withDocumentation("Easily configure one the built-in key generators, instead of specifying the key generator class."
          + "Currently supports SIMPLE, COMPLEX, TIMESTAMP, CUSTOM, NON_PARTITION, GLOBAL_DELETE");

  public static final ConfigProperty<String> ROLLBACK_USING_MARKERS = ConfigProperty
      .key("hoodie.rollback.using.markers")
      .defaultValue("false")
      .withDocumentation("Enables a more efficient mechanism for rollbacks based on the marker files generated "
          + "during the writes. Turned off by default.");

  public static final ConfigProperty<String> TIMELINE_LAYOUT_VERSION = ConfigProperty
      .key("hoodie.timeline.layout.version")
      .noDefaultValue()
      .sinceVersion("0.5.1")
      .withDocumentation("Controls the layout of the timeline. Version 0 relied on renames, Version 1 (default) models "
          + "the timeline as an immutable log relying only on atomic writes for object storage.");

  public static final ConfigProperty<String> BASE_PATH_PROP = ConfigProperty
      .key("hoodie.base.path")
      .noDefaultValue()
      .withDocumentation("Base path on lake storage, under which all the table data is stored. "
          + "Always prefix it explicitly with the storage scheme (e.g hdfs://, s3:// etc). "
          + "Hudi stores all the main meta-data about commits, savepoints, cleaning audit logs "
          + "etc in .hoodie directory under this base path directory.");

  public static final ConfigProperty<String> AVRO_SCHEMA = ConfigProperty
      .key("hoodie.avro.schema")
      .noDefaultValue()
      .withDocumentation("Schema string representing the current write schema of the table. Hudi passes this to "
          + "implementations of HoodieRecordPayload to convert incoming records to avro. This is also used as the write schema "
          + "evolving records during an update.");

  public static final ConfigProperty<String> AVRO_SCHEMA_VALIDATE = ConfigProperty
      .key("hoodie.avro.schema.validate")
      .defaultValue("false")
      .withDocumentation("Validate the schema used for the write against the latest schema, for backwards compatibility.");

  public static final ConfigProperty<String> INSERT_PARALLELISM = ConfigProperty
      .key("hoodie.insert.shuffle.parallelism")
      .defaultValue("1500")
      .withDocumentation("Parallelism for inserting records into the table. Inserts can shuffle data before writing to tune file sizes and optimize the storage layout.");

  public static final ConfigProperty<String> BULKINSERT_PARALLELISM = ConfigProperty
      .key("hoodie.bulkinsert.shuffle.parallelism")
      .defaultValue("1500")
      .withDocumentation("For large initial imports using bulk_insert operation, controls the parallelism to use for sort modes or custom partitioning done"
          + "before writing records to the table.");

  public static final ConfigProperty<String> BULKINSERT_USER_DEFINED_PARTITIONER_CLASS = ConfigProperty
      .key("hoodie.bulkinsert.user.defined.partitioner.class")
      .noDefaultValue()
      .withDocumentation("If specified, this class will be used to re-partition records before they are bulk inserted. This can be used to sort, pack, cluster data"
          + " optimally for common query patterns.");

  public static final ConfigProperty<String> UPSERT_PARALLELISM = ConfigProperty
      .key("hoodie.upsert.shuffle.parallelism")
      .defaultValue("1500")
      .withDocumentation("Parallelism to use for upsert operation on the table. Upserts can shuffle data to perform index lookups, file sizing, bin packing records optimally"
          + "into file groups.");

  public static final ConfigProperty<String> DELETE_PARALLELISM = ConfigProperty
      .key("hoodie.delete.shuffle.parallelism")
      .defaultValue("1500")
      .withDocumentation("Parallelism used for “delete” operation. Delete operations also performs shuffles, similar to upsert operation.");

  public static final ConfigProperty<String> ROLLBACK_PARALLELISM = ConfigProperty
      .key("hoodie.rollback.parallelism")
      .defaultValue("100")
      .withDocumentation("Parallelism for rollback of commits. Rollbacks perform delete of files or logging delete blocks to file groups on storage in parallel.");

  public static final ConfigProperty<String> WRITE_BUFFER_LIMIT_BYTES = ConfigProperty
      .key("hoodie.write.buffer.limit.bytes")
      .defaultValue(String.valueOf(4 * 1024 * 1024))
      .withDocumentation("Size of in-memory buffer used for parallelizing network reads and lake storage writes.");

  public static final ConfigProperty<String> COMBINE_BEFORE_INSERT_PROP = ConfigProperty
      .key("hoodie.combine.before.insert")
      .defaultValue("false")
      .withDocumentation("When inserted records share same key, controls whether they should be first combined (i.e de-duplicated) before"
          + " writing to storage.");

  public static final ConfigProperty<String> COMBINE_BEFORE_UPSERT_PROP = ConfigProperty
      .key("hoodie.combine.before.upsert")
      .defaultValue("true")
      .withDocumentation("When upserted records share same key, controls whether they should be first combined (i.e de-duplicated) before"
          + " writing to storage. This should be turned off only if you are absolutely certain that there are no duplicates incoming, "
          + " otherwise it can lead to duplicate keys and violate the uniqueness guarantees.");

  public static final ConfigProperty<String> COMBINE_BEFORE_DELETE_PROP = ConfigProperty
      .key("hoodie.combine.before.delete")
      .defaultValue("true")
      .withDocumentation("During delete operations, controls whether we should combine deletes (and potentially also upserts) before "
          + " writing to storage.");

  public static final ConfigProperty<String> WRITE_STATUS_STORAGE_LEVEL = ConfigProperty
      .key("hoodie.write.status.storage.level")
      .defaultValue("MEMORY_AND_DISK_SER")
      .withDocumentation("Write status objects hold metadata about a write (stats, errors), that is not yet committed to storage. "
          + "This controls the how that information is cached for inspection by clients. We rarely expect this to be changed.");

  public static final ConfigProperty<String> HOODIE_AUTO_COMMIT_PROP = ConfigProperty
      .key("hoodie.auto.commit")
      .defaultValue("true")
      .withDocumentation("Controls whether a write operation should auto commit. This can be turned off to perform inspection"
          + " of the uncommitted write before deciding to commit.");

  public static final ConfigProperty<String> HOODIE_WRITE_STATUS_CLASS_PROP = ConfigProperty
      .key("hoodie.writestatus.class")
      .defaultValue(WriteStatus.class.getName())
      .withDocumentation("Subclass of " + WriteStatus.class.getName() + " to be used to collect information about a write. Can be "
          + "overridden to collection additional metrics/statistics about the data if needed.");

  public static final ConfigProperty<String> FINALIZE_WRITE_PARALLELISM = ConfigProperty
      .key("hoodie.finalize.write.parallelism")
      .defaultValue("1500")
      .withDocumentation("Parallelism for the write finalization internal operation, which involves removing any partially written "
          + "files from lake storage, before committing the write. Reduce this value, if the high number of tasks incur delays for smaller tables "
          + "or low latency writes.");

  public static final ConfigProperty<String> MARKERS_DELETE_PARALLELISM = ConfigProperty
      .key("hoodie.markers.delete.parallelism")
      .defaultValue("100")
      .withDocumentation("Determines the parallelism for deleting marker files, which are used to track all files (valid or invalid/partial) written during "
          + "a write operation. Increase this value if delays are observed, with large batch writes.");

  public static final ConfigProperty<String> BULKINSERT_SORT_MODE = ConfigProperty
      .key("hoodie.bulkinsert.sort.mode")
      .defaultValue(BulkInsertSortMode.GLOBAL_SORT.toString())
      .withDocumentation("Sorting modes to use for sorting records for bulk insert. This is user when user "
          + BULKINSERT_USER_DEFINED_PARTITIONER_CLASS.key() + "is not configured. Available values are - "
          + "GLOBAL_SORT: this ensures best file sizes, with lowest memory overhead at cost of sorting. "
          + "PARTITION_SORT: Strikes a balance by only sorting within a partition, still keeping the memory overhead of writing "
          + "lowest and best effort file sizing. "
          + "NONE: No sorting. Fastest and matches `spark.write.parquet()` in terms of number of files, overheads");

  public static final ConfigProperty<String> EMBEDDED_TIMELINE_SERVER_ENABLED = ConfigProperty
      .key("hoodie.embed.timeline.server")
      .defaultValue("true")
      .withDocumentation("When true, spins up an instance of the timeline server (meta server that serves cached file listings, statistics),"
          + "running on each writer's driver process, accepting requests during the write from executors.");

  public static final ConfigProperty<String> EMBEDDED_TIMELINE_SERVER_REUSE_ENABLED = ConfigProperty
      .key("hoodie.embed.timeline.server.reuse.enabled")
      .defaultValue("false")
      .withDocumentation("Controls whether the timeline server instance should be cached and reused across the JVM (across task lifecycles)"
          + "to avoid startup costs. This should rarely be changed.");

  public static final ConfigProperty<String> EMBEDDED_TIMELINE_SERVER_PORT = ConfigProperty
      .key("hoodie.embed.timeline.server.port")
      .defaultValue("0")
      .withDocumentation("Port at which the timeline server listens for requests. When running embedded in each writer, it picks "
          + "a free port and communicates to all the executors. This should rarely be changed.");

  public static final ConfigProperty<String> EMBEDDED_TIMELINE_SERVER_THREADS = ConfigProperty
      .key("hoodie.embed.timeline.server.threads")
      .defaultValue("-1")
      .withDocumentation("Number of threads to serve requests in the timeline server. By default, auto configured based on the number of underlying cores.");

  public static final ConfigProperty<String> EMBEDDED_TIMELINE_SERVER_COMPRESS_OUTPUT = ConfigProperty
      .key("hoodie.embed.timeline.server.gzip")
      .defaultValue("true")
      .withDocumentation("Controls whether gzip compression is used, for large responses from the timeline server, to improve latency.");

  public static final ConfigProperty<String> EMBEDDED_TIMELINE_SERVER_USE_ASYNC = ConfigProperty
      .key("hoodie.embed.timeline.server.async")
      .defaultValue("false")
      .withDocumentation("Controls whether or not, the requests to the timeline server are processed in asynchronous fashion, "
          + "potentially improving throughput.");

  public static final ConfigProperty<String> FAIL_ON_TIMELINE_ARCHIVING_ENABLED_PROP = ConfigProperty
      .key("hoodie.fail.on.timeline.archiving")
      .defaultValue("true")
      .withDocumentation("Timeline archiving removes older instants from the timeline, after each write operation, to minimize metadata overhead. "
          + "Controls whether or not, the write should be failed as well, if such archiving fails.");

  public static final ConfigProperty<Long> INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP = ConfigProperty
      .key("hoodie.consistency.check.initial_interval_ms")
      .defaultValue(2000L)
      .withDocumentation("Initial time between successive attempts to ensure written data's metadata is consistent on storage. Grows with exponential"
          + " backoff after the initial value.");

  public static final ConfigProperty<Long> MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP = ConfigProperty
      .key("hoodie.consistency.check.max_interval_ms")
      .defaultValue(300000L)
      .withDocumentation("Max time to wait between successive attempts at performing consistency checks");

  public static final ConfigProperty<Integer> MAX_CONSISTENCY_CHECKS_PROP = ConfigProperty
      .key("hoodie.consistency.check.max_checks")
      .defaultValue(7)
      .withDocumentation("Maximum number of checks, for consistency of written data.");

  public static final ConfigProperty<String> MERGE_DATA_VALIDATION_CHECK_ENABLED = ConfigProperty
      .key("hoodie.merge.data.validation.enabled")
      .defaultValue("false")
      .withDocumentation("When enabled, data validation checks are performed during merges to ensure expected "
          + "number of records after merge operation.");

  public static final ConfigProperty<String> MERGE_ALLOW_DUPLICATE_ON_INSERTS = ConfigProperty
      .key("hoodie.merge.allow.duplicate.on.inserts")
      .defaultValue("false")
      .withDocumentation("When enabled, we allow duplicate keys even if inserts are routed to merge with an existing file (for ensuring file sizing)."
          + " This is only relevant for insert operation, since upsert, delete operations will ensure unique key constraints are maintained.");

  public static final ConfigProperty<ExternalSpillableMap.DiskMapType> SPILLABLE_DISK_MAP_TYPE = ConfigProperty
      .key("hoodie.spillable.diskmap.type")
      .defaultValue(ExternalSpillableMap.DiskMapType.BITCASK)
      .withDocumentation("When handling input data that cannot be held in memory, to merge with a file on storage, a spillable diskmap is employed.  "
          + "By default, we use a persistent hashmap based loosely on bitcask, that offers O(1) inserts, lookups. "
          + "Change this to `ROCKS_DB` to prefer using rocksDB, for handling the spill.");

  public static final ConfigProperty<Boolean> DISK_MAP_BITCASK_COMPRESSION_ENABLED = ConfigProperty
      .key("hoodie.diskmap.bitcask.compression.enabled")
      .defaultValue(true)
      .withDocumentation("Turn on compression for BITCASK disk map used by the External Spillable Map");

  public static final ConfigProperty<Integer> CLIENT_HEARTBEAT_INTERVAL_IN_MS_PROP = ConfigProperty
      .key("hoodie.client.heartbeat.interval_in_ms")
      .defaultValue(60 * 1000)
      .withDocumentation("Writers perform heartbeats to indicate liveness. Controls how often (in ms), such heartbeats are registered to lake storage.");

  public static final ConfigProperty<Integer> CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES_PROP = ConfigProperty
      .key("hoodie.client.heartbeat.tolerable.misses")
      .defaultValue(2)
      .withDocumentation("Number of heartbeat misses, before a writer is deemed not alive and all pending writes are aborted.");

  public static final ConfigProperty<String> WRITE_CONCURRENCY_MODE_PROP = ConfigProperty
      .key("hoodie.write.concurrency.mode")
      .defaultValue(WriteConcurrencyMode.SINGLE_WRITER.name())
      .withDocumentation("Enable different concurrency modes. Options are "
          + "SINGLE_WRITER: Only one active writer to the table. Maximizes throughput"
          + "OPTIMISTIC_CONCURRENCY_CONTROL: Multiple writers can operate on the table and exactly one of them succeed "
          + "if a conflict (writes affect the same file group) is detected.");

  public static final ConfigProperty<String> WRITE_META_KEY_PREFIXES_PROP = ConfigProperty
      .key("hoodie.write.meta.key.prefixes")
      .defaultValue("")
      .withDocumentation("Comma separated metadata key prefixes to override from latest commit "
          + "during overlapping commits via multi writing");

  public static final ConfigProperty<HoodieFileFormat> HOODIE_BASE_FILE_FORMAT_PROP = ConfigProperty
          .key("hoodie.table.base.file.format")
          .defaultValue(HoodieFileFormat.PARQUET)
          .withAlternatives("hoodie.table.ro.file.format")
          .withDocumentation("");


  /**
   * Currently the  use this to specify the write schema.
   */
  public static final ConfigProperty<String> WRITE_SCHEMA_PROP = ConfigProperty
      .key("hoodie.write.schema")
      .noDefaultValue()
      .withDocumentation("The specified write schema. In most case, we do not need set this parameter,"
          + " but for the case the write schema is not equal to the specified table schema, we can"
          + " specify the write schema by this parameter. Used by MergeIntoHoodieTableCommand");

  /**
   * HUDI-858 : There are users who had been directly using RDD APIs and have relied on a behavior in 0.4.x to allow
   * multiple write operations (upsert/buk-insert/...) to be executed within a single commit.
   * <p>
   * Given Hudi commit protocol, these are generally unsafe operations and user need to handle failure scenarios. It
   * only works with COW table. Hudi 0.5.x had stopped this behavior.
   * <p>
   * Given the importance of supporting such cases for the user's migration to 0.5.x, we are proposing a safety flag
   * (disabled by default) which will allow this old behavior.
   */
  public static final ConfigProperty<String> ALLOW_MULTI_WRITE_ON_SAME_INSTANT = ConfigProperty
      .key("_.hoodie.allow.multi.write.on.same.instant")
      .defaultValue("false")
      .withDocumentation("");

  public static final ConfigProperty<String> EXTERNAL_RECORD_AND_SCHEMA_TRANSFORMATION = ConfigProperty
      .key(AVRO_SCHEMA.key() + ".external.transformation")
      .defaultValue("false")
      .withAlternatives(AVRO_SCHEMA.key() + ".externalTransformation")
      .withDocumentation("When enabled, records in older schema are rewritten into newer schema during upsert,delete and background"
          + " compaction,clustering operations.");

  private ConsistencyGuardConfig consistencyGuardConfig;

  // Hoodie Write Client transparently rewrites File System View config when embedded mode is enabled
  // We keep track of original config and rewritten config
  private final FileSystemViewStorageConfig clientSpecifiedViewStorageConfig;
  private FileSystemViewStorageConfig viewStorageConfig;
  private HoodiePayloadConfig hoodiePayloadConfig;
  private HoodieMetadataConfig metadataConfig;
  private EngineType engineType;

  /**
   * Use Spark engine by default.
   */
  protected HoodieWriteConfig() {
    super();
    this.engineType = EngineType.SPARK;
    this.clientSpecifiedViewStorageConfig = null;
  }

  protected HoodieWriteConfig(EngineType engineType, Properties props) {
    super(props);
    Properties newProps = new Properties();
    newProps.putAll(props);
    this.engineType = engineType;
    this.consistencyGuardConfig = ConsistencyGuardConfig.newBuilder().fromProperties(newProps).build();
    this.clientSpecifiedViewStorageConfig = FileSystemViewStorageConfig.newBuilder().fromProperties(newProps).build();
    this.viewStorageConfig = clientSpecifiedViewStorageConfig;
    this.hoodiePayloadConfig = HoodiePayloadConfig.newBuilder().fromProperties(newProps).build();
    this.metadataConfig = HoodieMetadataConfig.newBuilder().fromProperties(props).build();
  }

  public static HoodieWriteConfig.Builder newBuilder() {
    return new Builder();
  }

  /**
   * base properties.
   */
  public String getBasePath() {
    return getString(BASE_PATH_PROP);
  }

  public String getSchema() {
    return getString(AVRO_SCHEMA);
  }

  public void setSchema(String schemaStr) {
    setValue(AVRO_SCHEMA, schemaStr);
  }

  /**
   * Get the write schema for written records.
   *
   * If the WRITE_SCHEMA has specified, we use the WRITE_SCHEMA.
   * Or else we use the AVRO_SCHEMA as the write schema.
   * @return
   */
  public String getWriteSchema() {
    if (props.containsKey(WRITE_SCHEMA_PROP.key())) {
      return getString(WRITE_SCHEMA_PROP);
    }
    return getSchema();
  }

  public boolean getAvroSchemaValidate() {
    return getBoolean(AVRO_SCHEMA_VALIDATE);
  }

  public String getTableName() {
    return getString(TABLE_NAME);
  }

  public String getPreCombineField() {
    return getString(PRECOMBINE_FIELD_PROP);
  }

  public String getWritePayloadClass() {
    return getString(WRITE_PAYLOAD_CLASS);
  }

  public String getKeyGeneratorClass() {
    return getString(KEYGENERATOR_CLASS_PROP);
  }

  public Boolean shouldAutoCommit() {
    return getBoolean(HOODIE_AUTO_COMMIT_PROP);
  }

  public Boolean shouldAssumeDatePartitioning() {
    return metadataConfig.shouldAssumeDatePartitioning();
  }

  public boolean shouldUseExternalSchemaTransformation() {
    return getBoolean(EXTERNAL_RECORD_AND_SCHEMA_TRANSFORMATION);
  }

  public Integer getTimelineLayoutVersion() {
    return getInt(TIMELINE_LAYOUT_VERSION);
  }

  public int getBulkInsertShuffleParallelism() {
    return getInt(BULKINSERT_PARALLELISM);
  }

  public String getUserDefinedBulkInsertPartitionerClass() {
    return getString(BULKINSERT_USER_DEFINED_PARTITIONER_CLASS);
  }

  public int getInsertShuffleParallelism() {
    return getInt(INSERT_PARALLELISM);
  }

  public int getUpsertShuffleParallelism() {
    return getInt(UPSERT_PARALLELISM);
  }

  public int getDeleteShuffleParallelism() {
    return Math.max(getInt(DELETE_PARALLELISM), 1);
  }

  public int getRollbackParallelism() {
    return getInt(ROLLBACK_PARALLELISM);
  }

  public int getFileListingParallelism() {
    return metadataConfig.getFileListingParallelism();
  }

  public boolean shouldRollbackUsingMarkers() {
    return getBoolean(ROLLBACK_USING_MARKERS);
  }

  public int getWriteBufferLimitBytes() {
    return Integer.parseInt(getStringOrDefault(WRITE_BUFFER_LIMIT_BYTES));
  }

  public boolean shouldCombineBeforeInsert() {
    return getBoolean(COMBINE_BEFORE_INSERT_PROP);
  }

  public boolean shouldCombineBeforeUpsert() {
    return getBoolean(COMBINE_BEFORE_UPSERT_PROP);
  }

  public boolean shouldCombineBeforeDelete() {
    return getBoolean(COMBINE_BEFORE_DELETE_PROP);
  }

  public boolean shouldAllowMultiWriteOnSameInstant() {
    return getBoolean(ALLOW_MULTI_WRITE_ON_SAME_INSTANT);
  }

  public String getWriteStatusClassName() {
    return getString(HOODIE_WRITE_STATUS_CLASS_PROP);
  }

  public int getFinalizeWriteParallelism() {
    return getInt(FINALIZE_WRITE_PARALLELISM);
  }

  public int getMarkersDeleteParallelism() {
    return getInt(MARKERS_DELETE_PARALLELISM);
  }

  public boolean isEmbeddedTimelineServerEnabled() {
    return getBoolean(EMBEDDED_TIMELINE_SERVER_ENABLED);
  }

  public boolean isEmbeddedTimelineServerReuseEnabled() {
    return Boolean.parseBoolean(getStringOrDefault(EMBEDDED_TIMELINE_SERVER_REUSE_ENABLED));
  }

  public int getEmbeddedTimelineServerPort() {
    return Integer.parseInt(getStringOrDefault(EMBEDDED_TIMELINE_SERVER_PORT));
  }

  public int getEmbeddedTimelineServerThreads() {
    return Integer.parseInt(getStringOrDefault(EMBEDDED_TIMELINE_SERVER_THREADS));
  }

  public boolean getEmbeddedTimelineServerCompressOutput() {
    return Boolean.parseBoolean(getStringOrDefault(EMBEDDED_TIMELINE_SERVER_COMPRESS_OUTPUT));
  }

  public boolean getEmbeddedTimelineServerUseAsync() {
    return Boolean.parseBoolean(getStringOrDefault(EMBEDDED_TIMELINE_SERVER_USE_ASYNC));
  }

  public boolean isFailOnTimelineArchivingEnabled() {
    return getBoolean(FAIL_ON_TIMELINE_ARCHIVING_ENABLED_PROP);
  }

  public int getMaxConsistencyChecks() {
    return getInt(MAX_CONSISTENCY_CHECKS_PROP);
  }

  public int getInitialConsistencyCheckIntervalMs() {
    return getInt(INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP);
  }

  public int getMaxConsistencyCheckIntervalMs() {
    return getInt(MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP);
  }

  public BulkInsertSortMode getBulkInsertSortMode() {
    String sortMode = getString(BULKINSERT_SORT_MODE);
    return BulkInsertSortMode.valueOf(sortMode.toUpperCase());
  }

  public boolean isMergeDataValidationCheckEnabled() {
    return getBoolean(MERGE_DATA_VALIDATION_CHECK_ENABLED);
  }

  public boolean allowDuplicateInserts() {
    return getBoolean(MERGE_ALLOW_DUPLICATE_ON_INSERTS);
  }

  public ExternalSpillableMap.DiskMapType getSpillableDiskMapType() {
    return ExternalSpillableMap.DiskMapType.valueOf(getString(SPILLABLE_DISK_MAP_TYPE).toUpperCase(Locale.ROOT));
  }

  public boolean isBitCaskDiskMapCompressionEnabled() {
    return getBoolean(DISK_MAP_BITCASK_COMPRESSION_ENABLED);
  }

  public EngineType getEngineType() {
    return engineType;
  }

  /**
   * compaction properties.
   */
  public HoodieCleaningPolicy getCleanerPolicy() {
    return HoodieCleaningPolicy.valueOf(getString(HoodieCompactionConfig.CLEANER_POLICY_PROP));
  }

  public int getCleanerFileVersionsRetained() {
    return getInt(HoodieCompactionConfig.CLEANER_FILE_VERSIONS_RETAINED_PROP);
  }

  public int getCleanerCommitsRetained() {
    return getInt(HoodieCompactionConfig.CLEANER_COMMITS_RETAINED_PROP);
  }

  public int getMaxCommitsToKeep() {
    return getInt(HoodieCompactionConfig.MAX_COMMITS_TO_KEEP_PROP);
  }

  public int getMinCommitsToKeep() {
    return getInt(HoodieCompactionConfig.MIN_COMMITS_TO_KEEP_PROP);
  }

  public int getParquetSmallFileLimit() {
    return getInt(HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT_BYTES);
  }

  public double getRecordSizeEstimationThreshold() {
    return getDouble(HoodieCompactionConfig.RECORD_SIZE_ESTIMATION_THRESHOLD_PROP);
  }

  public int getCopyOnWriteInsertSplitSize() {
    return getInt(HoodieCompactionConfig.COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE);
  }

  public int getCopyOnWriteRecordSizeEstimate() {
    return getInt(HoodieCompactionConfig.COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE);
  }

  public boolean shouldAutoTuneInsertSplits() {
    return getBoolean(HoodieCompactionConfig.COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS);
  }

  public int getCleanerParallelism() {
    return getInt(HoodieCompactionConfig.CLEANER_PARALLELISM);
  }

  public boolean isAutoClean() {
    return getBoolean(HoodieCompactionConfig.AUTO_CLEAN_PROP);
  }

  public boolean isAsyncClean() {
    return getBoolean(HoodieCompactionConfig.ASYNC_CLEAN_PROP);
  }

  public boolean incrementalCleanerModeEnabled() {
    return getBoolean(HoodieCompactionConfig.CLEANER_INCREMENTAL_MODE);
  }

  public boolean inlineCompactionEnabled() {
    return getBoolean(HoodieCompactionConfig.INLINE_COMPACT_PROP);
  }

  public CompactionTriggerStrategy getInlineCompactTriggerStrategy() {
    return CompactionTriggerStrategy.valueOf(getString(HoodieCompactionConfig.INLINE_COMPACT_TRIGGER_STRATEGY_PROP));
  }

  public int getInlineCompactDeltaCommitMax() {
    return getInt(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS_PROP);
  }

  public int getInlineCompactDeltaSecondsMax() {
    return getInt(HoodieCompactionConfig.INLINE_COMPACT_TIME_DELTA_SECONDS_PROP);
  }

  public CompactionStrategy getCompactionStrategy() {
    return ReflectionUtils.loadClass(getString(HoodieCompactionConfig.COMPACTION_STRATEGY_PROP));
  }

  public Long getTargetIOPerCompactionInMB() {
    return getLong(HoodieCompactionConfig.TARGET_IO_PER_COMPACTION_IN_MB_PROP);
  }

  public Boolean getCompactionLazyBlockReadEnabled() {
    return getBoolean(HoodieCompactionConfig.COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP);
  }

  public Boolean getCompactionReverseLogReadEnabled() {
    return getBoolean(HoodieCompactionConfig.COMPACTION_REVERSE_LOG_READ_ENABLED_PROP);
  }

  public boolean inlineClusteringEnabled() {
    return getBoolean(HoodieClusteringConfig.INLINE_CLUSTERING_PROP);
  }

  public boolean isAsyncClusteringEnabled() {
    return getBoolean(HoodieClusteringConfig.ASYNC_CLUSTERING_ENABLE_OPT_KEY);
  }

  public boolean isClusteringEnabled() {
    // TODO: future support async clustering
    return inlineClusteringEnabled() || isAsyncClusteringEnabled();
  }

  public int getInlineClusterMaxCommits() {
    return getInt(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMIT_PROP);
  }

  public int getAsyncClusterMaxCommits() {
    return getInt(HoodieClusteringConfig.ASYNC_CLUSTERING_MAX_COMMIT_PROP);
  }

  public String getPayloadClass() {
    return getString(HoodieCompactionConfig.PAYLOAD_CLASS_PROP);
  }

  public int getTargetPartitionsPerDayBasedCompaction() {
    return getInt(HoodieCompactionConfig.TARGET_PARTITIONS_PER_DAYBASED_COMPACTION_PROP);
  }

  public int getCommitArchivalBatchSize() {
    return getInt(HoodieCompactionConfig.COMMITS_ARCHIVAL_BATCH_SIZE_PROP);
  }

  public Boolean shouldCleanBootstrapBaseFile() {
    return getBoolean(HoodieCompactionConfig.CLEANER_BOOTSTRAP_BASE_FILE_ENABLED);
  }

  public String getClusteringUpdatesStrategyClass() {
    return getString(HoodieClusteringConfig.CLUSTERING_UPDATES_STRATEGY_PROP);
  }

  public HoodieFailedWritesCleaningPolicy getFailedWritesCleanPolicy() {
    return HoodieFailedWritesCleaningPolicy
        .valueOf(getString(HoodieCompactionConfig.FAILED_WRITES_CLEANER_POLICY_PROP));
  }

  /**
   * Clustering properties.
   */
  public String getClusteringPlanStrategyClass() {
    return getString(HoodieClusteringConfig.CLUSTERING_PLAN_STRATEGY_CLASS);
  }

  public String getClusteringExecutionStrategyClass() {
    return getString(HoodieClusteringConfig.CLUSTERING_EXECUTION_STRATEGY_CLASS);
  }

  public long getClusteringMaxBytesInGroup() {
    return getLong(HoodieClusteringConfig.CLUSTERING_MAX_BYTES_PER_GROUP);
  }

  public long getClusteringSmallFileLimit() {
    return getLong(HoodieClusteringConfig.CLUSTERING_PLAN_SMALL_FILE_LIMIT);
  }

  public int getClusteringMaxNumGroups() {
    return getInt(HoodieClusteringConfig.CLUSTERING_MAX_NUM_GROUPS);
  }

  public long getClusteringTargetFileMaxBytes() {
    return getLong(HoodieClusteringConfig.CLUSTERING_TARGET_FILE_MAX_BYTES);
  }

  public int getTargetPartitionsForClustering() {
    return getInt(HoodieClusteringConfig.CLUSTERING_TARGET_PARTITIONS);
  }

  public String getClusteringSortColumns() {
    return getString(HoodieClusteringConfig.CLUSTERING_SORT_COLUMNS_PROPERTY);
  }

  /**
   * index properties.
   */
  public HoodieIndex.IndexType getIndexType() {
    return HoodieIndex.IndexType.valueOf(getString(HoodieIndexConfig.INDEX_TYPE_PROP));
  }

  public String getIndexClass() {
    return getString(HoodieIndexConfig.INDEX_CLASS_PROP);
  }

  public int getBloomFilterNumEntries() {
    return getInt(HoodieIndexConfig.BLOOM_FILTER_NUM_ENTRIES);
  }

  public double getBloomFilterFPP() {
    return getDouble(HoodieIndexConfig.BLOOM_FILTER_FPP);
  }

  public String getHbaseZkQuorum() {
    return getString(HoodieHBaseIndexConfig.HBASE_ZKQUORUM_PROP);
  }

  public int getHbaseZkPort() {
    return getInt(HoodieHBaseIndexConfig.HBASE_ZKPORT_PROP);
  }

  public String getHBaseZkZnodeParent() {
    return getString(HoodieHBaseIndexConfig.HBASE_ZK_ZNODEPARENT);
  }

  public String getHbaseTableName() {
    return getString(HoodieHBaseIndexConfig.HBASE_TABLENAME_PROP);
  }

  public int getHbaseIndexGetBatchSize() {
    return getInt(HoodieHBaseIndexConfig.HBASE_GET_BATCH_SIZE_PROP);
  }

  public Boolean getHBaseIndexRollbackSync() {
    return getBoolean(HoodieHBaseIndexConfig.HBASE_INDEX_ROLLBACK_SYNC);
  }

  public int getHbaseIndexPutBatchSize() {
    return getInt(HoodieHBaseIndexConfig.HBASE_PUT_BATCH_SIZE_PROP);
  }

  public Boolean getHbaseIndexPutBatchSizeAutoCompute() {
    return getBoolean(HoodieHBaseIndexConfig.HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE_PROP);
  }

  public String getHBaseQPSResourceAllocatorClass() {
    return getString(HoodieHBaseIndexConfig.HBASE_INDEX_QPS_ALLOCATOR_CLASS);
  }

  public String getHBaseQPSZKnodePath() {
    return getString(HoodieHBaseIndexConfig.HBASE_ZK_PATH_QPS_ROOT);
  }

  public String getHBaseZkZnodeSessionTimeout() {
    return getString(HoodieHBaseIndexConfig.HOODIE_INDEX_HBASE_ZK_SESSION_TIMEOUT_MS);
  }

  public String getHBaseZkZnodeConnectionTimeout() {
    return getString(HoodieHBaseIndexConfig.HOODIE_INDEX_HBASE_ZK_CONNECTION_TIMEOUT_MS);
  }

  public boolean getHBaseIndexShouldComputeQPSDynamically() {
    return getBoolean(HoodieHBaseIndexConfig.HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY);
  }

  public int getHBaseIndexDesiredPutsTime() {
    return getInt(HoodieHBaseIndexConfig.HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECS);
  }

  public String getBloomFilterType() {
    return getString(HoodieIndexConfig.BLOOM_INDEX_FILTER_TYPE);
  }

  public int getDynamicBloomFilterMaxNumEntries() {
    return getInt(HoodieIndexConfig.HOODIE_BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES);
  }

  /**
   * Fraction of the global share of QPS that should be allocated to this job. Let's say there are 3 jobs which have
   * input size in terms of number of rows required for HbaseIndexing as x, 2x, 3x respectively. Then this fraction for
   * the jobs would be (0.17) 1/6, 0.33 (2/6) and 0.5 (3/6) respectively.
   */
  public float getHbaseIndexQPSFraction() {
    return getFloat(HoodieHBaseIndexConfig.HBASE_QPS_FRACTION_PROP);
  }

  public float getHBaseIndexMinQPSFraction() {
    return getFloat(HoodieHBaseIndexConfig.HBASE_MIN_QPS_FRACTION_PROP);
  }

  public float getHBaseIndexMaxQPSFraction() {
    return getFloat(HoodieHBaseIndexConfig.HBASE_MAX_QPS_FRACTION_PROP);
  }

  /**
   * This should be same across various jobs. This is intended to limit the aggregate QPS generated across various
   * Hoodie jobs to an Hbase Region Server
   */
  public int getHbaseIndexMaxQPSPerRegionServer() {
    return getInt(HoodieHBaseIndexConfig.HBASE_MAX_QPS_PER_REGION_SERVER_PROP);
  }

  public boolean getHbaseIndexUpdatePartitionPath() {
    return getBoolean(HoodieHBaseIndexConfig.HBASE_INDEX_UPDATE_PARTITION_PATH);
  }

  public int getBloomIndexParallelism() {
    return getInt(HoodieIndexConfig.BLOOM_INDEX_PARALLELISM_PROP);
  }

  public boolean getBloomIndexPruneByRanges() {
    return getBoolean(HoodieIndexConfig.BLOOM_INDEX_PRUNE_BY_RANGES_PROP);
  }

  public boolean getBloomIndexUseCaching() {
    return getBoolean(HoodieIndexConfig.BLOOM_INDEX_USE_CACHING_PROP);
  }

  public boolean useBloomIndexTreebasedFilter() {
    return getBoolean(HoodieIndexConfig.BLOOM_INDEX_TREE_BASED_FILTER_PROP);
  }

  public boolean useBloomIndexBucketizedChecking() {
    return getBoolean(HoodieIndexConfig.BLOOM_INDEX_BUCKETIZED_CHECKING_PROP);
  }

  public int getBloomIndexKeysPerBucket() {
    return getInt(HoodieIndexConfig.BLOOM_INDEX_KEYS_PER_BUCKET_PROP);
  }

  public boolean getBloomIndexUpdatePartitionPath() {
    return getBoolean(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH);
  }

  public int getSimpleIndexParallelism() {
    return getInt(HoodieIndexConfig.SIMPLE_INDEX_PARALLELISM_PROP);
  }

  public boolean getSimpleIndexUseCaching() {
    return getBoolean(HoodieIndexConfig.SIMPLE_INDEX_USE_CACHING_PROP);
  }

  public int getGlobalSimpleIndexParallelism() {
    return getInt(HoodieIndexConfig.GLOBAL_SIMPLE_INDEX_PARALLELISM_PROP);
  }

  public boolean getGlobalSimpleIndexUpdatePartitionPath() {
    return getBoolean(HoodieIndexConfig.SIMPLE_INDEX_UPDATE_PARTITION_PATH);
  }

  /**
   * storage properties.
   */
  public long getParquetMaxFileSize() {
    return getLong(HoodieStorageConfig.PARQUET_FILE_MAX_BYTES);
  }

  public int getParquetBlockSize() {
    return getInt(HoodieStorageConfig.PARQUET_BLOCK_SIZE_BYTES);
  }

  public int getParquetPageSize() {
    return getInt(HoodieStorageConfig.PARQUET_PAGE_SIZE_BYTES);
  }

  public int getLogFileDataBlockMaxSize() {
    return getInt(HoodieStorageConfig.LOGFILE_DATA_BLOCK_SIZE_MAX_BYTES);
  }

  public int getLogFileMaxSize() {
    return getInt(HoodieStorageConfig.LOGFILE_SIZE_MAX_BYTES);
  }

  public double getParquetCompressionRatio() {
    return getDouble(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO);
  }

  public CompressionCodecName getParquetCompressionCodec() {
    return CompressionCodecName.fromConf(getString(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC));
  }

  public double getLogFileToParquetCompressionRatio() {
    return getDouble(HoodieStorageConfig.LOGFILE_TO_PARQUET_COMPRESSION_RATIO);
  }

  public long getHFileMaxFileSize() {
    return getLong(HoodieStorageConfig.HFILE_FILE_MAX_BYTES);
  }

  public int getHFileBlockSize() {
    return getInt(HoodieStorageConfig.HFILE_BLOCK_SIZE_BYTES);
  }

  public Compression.Algorithm getHFileCompressionAlgorithm() {
    return Compression.Algorithm.valueOf(getString(HoodieStorageConfig.HFILE_COMPRESSION_ALGORITHM));
  }

  public long getOrcMaxFileSize() {
    return getLong(HoodieStorageConfig.ORC_FILE_MAX_BYTES);
  }

  public int getOrcStripeSize() {
    return getInt(HoodieStorageConfig.ORC_STRIPE_SIZE);
  }

  public int getOrcBlockSize() {
    return getInt(HoodieStorageConfig.ORC_BLOCK_SIZE);
  }

  public CompressionKind getOrcCompressionCodec() {
    return CompressionKind.valueOf(getString(HoodieStorageConfig.ORC_COMPRESSION_CODEC));
  }

  /**
   * metrics properties.
   */
  public boolean isMetricsOn() {
    return getBoolean(HoodieMetricsConfig.METRICS_ON);
  }

  public boolean isExecutorMetricsEnabled() {
    return Boolean.parseBoolean(
        getStringOrDefault(HoodieMetricsConfig.ENABLE_EXECUTOR_METRICS, "false"));
  }

  public MetricsReporterType getMetricsReporterType() {
    return MetricsReporterType.valueOf(getString(HoodieMetricsConfig.METRICS_REPORTER_TYPE));
  }

  public String getGraphiteServerHost() {
    return getString(HoodieMetricsConfig.GRAPHITE_SERVER_HOST);
  }

  public int getGraphiteServerPort() {
    return getInt(HoodieMetricsConfig.GRAPHITE_SERVER_PORT);
  }

  public String getGraphiteMetricPrefix() {
    return getString(HoodieMetricsConfig.GRAPHITE_METRIC_PREFIX);
  }

  public String getJmxHost() {
    return getString(HoodieMetricsConfig.JMX_HOST);
  }

  public String getJmxPort() {
    return getString(HoodieMetricsConfig.JMX_PORT);
  }

  public int getDatadogReportPeriodSeconds() {
    return getInt(HoodieMetricsDatadogConfig.DATADOG_REPORT_PERIOD_SECONDS);
  }

  public ApiSite getDatadogApiSite() {
    return ApiSite.valueOf(getString(HoodieMetricsDatadogConfig.DATADOG_API_SITE));
  }

  public String getDatadogApiKey() {
    if (props.containsKey(HoodieMetricsDatadogConfig.DATADOG_API_KEY.key())) {
      return getString(HoodieMetricsDatadogConfig.DATADOG_API_KEY);
    } else {
      Supplier<String> apiKeySupplier = ReflectionUtils.loadClass(
          getString(HoodieMetricsDatadogConfig.DATADOG_API_KEY_SUPPLIER));
      return apiKeySupplier.get();
    }
  }

  public boolean getDatadogApiKeySkipValidation() {
    return getBoolean(HoodieMetricsDatadogConfig.DATADOG_API_KEY_SKIP_VALIDATION);
  }

  public int getDatadogApiTimeoutSeconds() {
    return getInt(HoodieMetricsDatadogConfig.DATADOG_API_TIMEOUT_SECONDS);
  }

  public String getDatadogMetricPrefix() {
    return getString(HoodieMetricsDatadogConfig.DATADOG_METRIC_PREFIX);
  }

  public String getDatadogMetricHost() {
    return getString(HoodieMetricsDatadogConfig.DATADOG_METRIC_HOST);
  }

  public List<String> getDatadogMetricTags() {
    return Arrays.stream(getStringOrDefault(
        HoodieMetricsDatadogConfig.DATADOG_METRIC_TAGS, ",").split("\\s*,\\s*")).collect(Collectors.toList());
  }

  public String getMetricReporterClassName() {
    return getString(HoodieMetricsConfig.METRICS_REPORTER_CLASS);
  }

  public int getPrometheusPort() {
    return getInt(HoodieMetricsPrometheusConfig.PROMETHEUS_PORT);
  }

  public String getPushGatewayHost() {
    return getString(HoodieMetricsPrometheusConfig.PUSHGATEWAY_HOST);
  }

  public int getPushGatewayPort() {
    return getInt(HoodieMetricsPrometheusConfig.PUSHGATEWAY_PORT);
  }

  public int getPushGatewayReportPeriodSeconds() {
    return getInt(HoodieMetricsPrometheusConfig.PUSHGATEWAY_REPORT_PERIOD_SECONDS);
  }

  public boolean getPushGatewayDeleteOnShutdown() {
    return getBoolean(HoodieMetricsPrometheusConfig.PUSHGATEWAY_DELETE_ON_SHUTDOWN);
  }

  public String getPushGatewayJobName() {
    return getString(HoodieMetricsPrometheusConfig.PUSHGATEWAY_JOB_NAME);
  }

  public boolean getPushGatewayRandomJobNameSuffix() {
    return getBoolean(HoodieMetricsPrometheusConfig.PUSHGATEWAY_RANDOM_JOB_NAME_SUFFIX);
  }

  /**
   * memory configs.
   */
  public int getMaxDFSStreamBufferSize() {
    return getInt(HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE_PROP);
  }

  public String getSpillableMapBasePath() {
    return getString(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH_PROP);
  }

  public double getWriteStatusFailureFraction() {
    return getDouble(HoodieMemoryConfig.WRITESTATUS_FAILURE_FRACTION_PROP);
  }

  public ConsistencyGuardConfig getConsistencyGuardConfig() {
    return consistencyGuardConfig;
  }

  public void setConsistencyGuardConfig(ConsistencyGuardConfig consistencyGuardConfig) {
    this.consistencyGuardConfig = consistencyGuardConfig;
  }

  public FileSystemViewStorageConfig getViewStorageConfig() {
    return viewStorageConfig;
  }

  public void setViewStorageConfig(FileSystemViewStorageConfig viewStorageConfig) {
    this.viewStorageConfig = viewStorageConfig;
  }

  public void resetViewStorageConfig() {
    this.setViewStorageConfig(getClientSpecifiedViewStorageConfig());
  }

  public FileSystemViewStorageConfig getClientSpecifiedViewStorageConfig() {
    return clientSpecifiedViewStorageConfig;
  }

  public HoodiePayloadConfig getPayloadConfig() {
    return hoodiePayloadConfig;
  }

  public HoodieMetadataConfig getMetadataConfig() {
    return metadataConfig;
  }

  /**
   * Commit call back configs.
   */
  public boolean writeCommitCallbackOn() {
    return getBoolean(HoodieWriteCommitCallbackConfig.CALLBACK_ON);
  }

  public String getCallbackClass() {
    return getString(HoodieWriteCommitCallbackConfig.CALLBACK_CLASS_PROP);
  }

  public String getBootstrapSourceBasePath() {
    return getString(HoodieBootstrapConfig.BOOTSTRAP_BASE_PATH_PROP);
  }

  public String getBootstrapModeSelectorClass() {
    return getString(HoodieBootstrapConfig.BOOTSTRAP_MODE_SELECTOR);
  }

  public String getFullBootstrapInputProvider() {
    return getString(HoodieBootstrapConfig.FULL_BOOTSTRAP_INPUT_PROVIDER);
  }

  public String getBootstrapKeyGeneratorClass() {
    return getString(HoodieBootstrapConfig.BOOTSTRAP_KEYGEN_CLASS);
  }

  public String getBootstrapKeyGeneratorType() {
    return getString(HoodieBootstrapConfig.BOOTSTRAP_KEYGEN_TYPE);
  }

  public String getBootstrapModeSelectorRegex() {
    return getString(HoodieBootstrapConfig.BOOTSTRAP_MODE_SELECTOR_REGEX);
  }

  public BootstrapMode getBootstrapModeForRegexMatch() {
    return BootstrapMode.valueOf(getString(HoodieBootstrapConfig.BOOTSTRAP_MODE_SELECTOR_REGEX_MODE));
  }

  public String getBootstrapPartitionPathTranslatorClass() {
    return getString(HoodieBootstrapConfig.BOOTSTRAP_PARTITION_PATH_TRANSLATOR_CLASS);
  }

  public int getBootstrapParallelism() {
    return getInt(HoodieBootstrapConfig.BOOTSTRAP_PARALLELISM);
  }

  public Long getMaxMemoryPerPartitionMerge() {
    return getLong(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE_PROP);
  }

  public Long getHoodieClientHeartbeatIntervalInMs() {
    return getLong(CLIENT_HEARTBEAT_INTERVAL_IN_MS_PROP);
  }

  public Integer getHoodieClientHeartbeatTolerableMisses() {
    return getInt(CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES_PROP);
  }

  /**
   * File listing metadata configs.
   */
  public boolean useFileListingMetadata() {
    return metadataConfig.useFileListingMetadata();
  }

  public boolean getFileListingMetadataVerify() {
    return metadataConfig.validateFileListingMetadata();
  }

  public int getMetadataInsertParallelism() {
    return getInt(HoodieMetadataConfig.METADATA_INSERT_PARALLELISM_PROP);
  }

  public int getMetadataCompactDeltaCommitMax() {
    return getInt(HoodieMetadataConfig.METADATA_COMPACT_NUM_DELTA_COMMITS_PROP);
  }

  public boolean isMetadataAsyncClean() {
    return getBoolean(HoodieMetadataConfig.METADATA_ASYNC_CLEAN_PROP);
  }

  public int getMetadataMaxCommitsToKeep() {
    return getInt(HoodieMetadataConfig.MAX_COMMITS_TO_KEEP_PROP);
  }

  public int getMetadataMinCommitsToKeep() {
    return getInt(HoodieMetadataConfig.MIN_COMMITS_TO_KEEP_PROP);
  }

  public int getMetadataCleanerCommitsRetained() {
    return getInt(HoodieMetadataConfig.CLEANER_COMMITS_RETAINED_PROP);
  }

  /**
   * Hoodie Client Lock Configs.
   * @return
   */

  public String getLockProviderClass() {
    return getString(HoodieLockConfig.LOCK_PROVIDER_CLASS_PROP);
  }

  public String getLockHiveDatabaseName() {
    return getString(HoodieLockConfig.HIVE_DATABASE_NAME_PROP);
  }

  public String getLockHiveTableName() {
    return getString(HoodieLockConfig.HIVE_TABLE_NAME_PROP);
  }

  public ConflictResolutionStrategy getWriteConflictResolutionStrategy() {
    return ReflectionUtils.loadClass(getString(HoodieLockConfig.WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_PROP));
  }

  public Long getLockAcquireWaitTimeoutInMs() {
    return getLong(HoodieLockConfig.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP);
  }

  public WriteConcurrencyMode getWriteConcurrencyMode() {
    return WriteConcurrencyMode.fromValue(getString(WRITE_CONCURRENCY_MODE_PROP));
  }

  public HoodieFileFormat getBaseFileFormat() {
    return HOODIE_BASE_FILE_FORMAT_PROP.defaultValue();
  }
  
  public Boolean inlineTableServices() {
    return inlineClusteringEnabled() || inlineCompactionEnabled() || isAutoClean();
  }

  public String getWriteMetaKeyPrefixes() {
    return getString(WRITE_META_KEY_PREFIXES_PROP);
  }

  public static class Builder {

    protected final HoodieWriteConfig writeConfig = new HoodieWriteConfig();
    protected EngineType engineType = EngineType.SPARK;
    private boolean isIndexConfigSet = false;
    private boolean isStorageConfigSet = false;
    private boolean isCompactionConfigSet = false;
    private boolean isClusteringConfigSet = false;
    private boolean isMetricsConfigSet = false;
    private boolean isBootstrapConfigSet = false;
    private boolean isMemoryConfigSet = false;
    private boolean isViewConfigSet = false;
    private boolean isConsistencyGuardSet = false;
    private boolean isCallbackConfigSet = false;
    private boolean isPayloadConfigSet = false;
    private boolean isMetadataConfigSet = false;
    private boolean isLockConfigSet = false;

    public Builder withEngineType(EngineType engineType) {
      this.engineType = engineType;
      return this;
    }

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.writeConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder fromInputStream(InputStream inputStream) throws IOException {
      try {
        this.writeConfig.getProps().load(inputStream);
        return this;
      } finally {
        inputStream.close();
      }
    }

    public Builder withProps(Map kvprops) {
      writeConfig.getProps().putAll(kvprops);
      return this;
    }

    public Builder withPath(String basePath) {
      writeConfig.setValue(BASE_PATH_PROP, basePath);
      return this;
    }

    public Builder withSchema(String schemaStr) {
      writeConfig.setValue(AVRO_SCHEMA, schemaStr);
      return this;
    }

    public Builder withAvroSchemaValidate(boolean enable) {
      writeConfig.setValue(AVRO_SCHEMA_VALIDATE, String.valueOf(enable));
      return this;
    }

    public Builder forTable(String tableName) {
      writeConfig.setValue(TABLE_NAME, tableName);
      return this;
    }

    public Builder withPreCombineField(String preCombineField) {
      writeConfig.setValue(PRECOMBINE_FIELD_PROP, preCombineField);
      return this;
    }

    public Builder withWritePayLoad(String payload) {
      writeConfig.setValue(WRITE_PAYLOAD_CLASS, payload);
      return this;
    }

    public Builder withKeyGenerator(String keyGeneratorClass) {
      writeConfig.setValue(KEYGENERATOR_CLASS_PROP, keyGeneratorClass);
      return this;
    }

    public Builder withTimelineLayoutVersion(int version) {
      writeConfig.setValue(TIMELINE_LAYOUT_VERSION, String.valueOf(version));
      return this;
    }

    public Builder withBulkInsertParallelism(int bulkInsertParallelism) {
      writeConfig.setValue(BULKINSERT_PARALLELISM, String.valueOf(bulkInsertParallelism));
      return this;
    }

    public Builder withUserDefinedBulkInsertPartitionerClass(String className) {
      writeConfig.setValue(BULKINSERT_USER_DEFINED_PARTITIONER_CLASS, className);
      return this;
    }

    public Builder withDeleteParallelism(int parallelism) {
      writeConfig.setValue(DELETE_PARALLELISM, String.valueOf(parallelism));
      return this;
    }

    public Builder withParallelism(int insertShuffleParallelism, int upsertShuffleParallelism) {
      writeConfig.setValue(INSERT_PARALLELISM, String.valueOf(insertShuffleParallelism));
      writeConfig.setValue(UPSERT_PARALLELISM, String.valueOf(upsertShuffleParallelism));
      return this;
    }

    public Builder withRollbackParallelism(int rollbackParallelism) {
      writeConfig.setValue(ROLLBACK_PARALLELISM, String.valueOf(rollbackParallelism));
      return this;
    }

    public Builder withRollbackUsingMarkers(boolean rollbackUsingMarkers) {
      writeConfig.setValue(ROLLBACK_USING_MARKERS, String.valueOf(rollbackUsingMarkers));
      return this;
    }

    public Builder withWriteBufferLimitBytes(int writeBufferLimit) {
      writeConfig.setValue(WRITE_BUFFER_LIMIT_BYTES, String.valueOf(writeBufferLimit));
      return this;
    }

    public Builder combineInput(boolean onInsert, boolean onUpsert) {
      writeConfig.setValue(COMBINE_BEFORE_INSERT_PROP, String.valueOf(onInsert));
      writeConfig.setValue(COMBINE_BEFORE_UPSERT_PROP, String.valueOf(onUpsert));
      return this;
    }

    public Builder combineDeleteInput(boolean onDelete) {
      writeConfig.setValue(COMBINE_BEFORE_DELETE_PROP, String.valueOf(onDelete));
      return this;
    }

    public Builder withWriteStatusStorageLevel(String level) {
      writeConfig.setValue(WRITE_STATUS_STORAGE_LEVEL, level);
      return this;
    }

    public Builder withIndexConfig(HoodieIndexConfig indexConfig) {
      writeConfig.getProps().putAll(indexConfig.getProps());
      isIndexConfigSet = true;
      return this;
    }

    public Builder withStorageConfig(HoodieStorageConfig storageConfig) {
      writeConfig.getProps().putAll(storageConfig.getProps());
      isStorageConfigSet = true;
      return this;
    }

    public Builder withCompactionConfig(HoodieCompactionConfig compactionConfig) {
      writeConfig.getProps().putAll(compactionConfig.getProps());
      isCompactionConfigSet = true;
      return this;
    }

    public Builder withClusteringConfig(HoodieClusteringConfig clusteringConfig) {
      writeConfig.getProps().putAll(clusteringConfig.getProps());
      isClusteringConfigSet = true;
      return this;
    }

    public Builder withLockConfig(HoodieLockConfig lockConfig) {
      writeConfig.getProps().putAll(lockConfig.getProps());
      isLockConfigSet = true;
      return this;
    }

    public Builder withMetricsConfig(HoodieMetricsConfig metricsConfig) {
      writeConfig.getProps().putAll(metricsConfig.getProps());
      isMetricsConfigSet = true;
      return this;
    }

    public Builder withMemoryConfig(HoodieMemoryConfig memoryConfig) {
      writeConfig.getProps().putAll(memoryConfig.getProps());
      isMemoryConfigSet = true;
      return this;
    }

    public Builder withBootstrapConfig(HoodieBootstrapConfig bootstrapConfig) {
      writeConfig.getProps().putAll(bootstrapConfig.getProps());
      isBootstrapConfigSet = true;
      return this;
    }

    public Builder withPayloadConfig(HoodiePayloadConfig payloadConfig) {
      writeConfig.getProps().putAll(payloadConfig.getProps());
      isPayloadConfigSet = true;
      return this;
    }

    public Builder withMetadataConfig(HoodieMetadataConfig metadataConfig) {
      writeConfig.getProps().putAll(metadataConfig.getProps());
      isMetadataConfigSet = true;
      return this;
    }

    public Builder withAutoCommit(boolean autoCommit) {
      writeConfig.setValue(HOODIE_AUTO_COMMIT_PROP, String.valueOf(autoCommit));
      return this;
    }

    public Builder withWriteStatusClass(Class<? extends WriteStatus> writeStatusClass) {
      writeConfig.setValue(HOODIE_WRITE_STATUS_CLASS_PROP, writeStatusClass.getName());
      return this;
    }

    public Builder withFileSystemViewConfig(FileSystemViewStorageConfig viewStorageConfig) {
      writeConfig.getProps().putAll(viewStorageConfig.getProps());
      isViewConfigSet = true;
      return this;
    }

    public Builder withConsistencyGuardConfig(ConsistencyGuardConfig consistencyGuardConfig) {
      writeConfig.getProps().putAll(consistencyGuardConfig.getProps());
      isConsistencyGuardSet = true;
      return this;
    }

    public Builder withCallbackConfig(HoodieWriteCommitCallbackConfig callbackConfig) {
      writeConfig.getProps().putAll(callbackConfig.getProps());
      isCallbackConfigSet = true;
      return this;
    }

    public Builder withFinalizeWriteParallelism(int parallelism) {
      writeConfig.setValue(FINALIZE_WRITE_PARALLELISM, String.valueOf(parallelism));
      return this;
    }

    public Builder withMarkersDeleteParallelism(int parallelism) {
      writeConfig.setValue(MARKERS_DELETE_PARALLELISM, String.valueOf(parallelism));
      return this;
    }

    public Builder withEmbeddedTimelineServerEnabled(boolean enabled) {
      writeConfig.setValue(EMBEDDED_TIMELINE_SERVER_ENABLED, String.valueOf(enabled));
      return this;
    }

    public Builder withEmbeddedTimelineServerReuseEnabled(boolean enabled) {
      writeConfig.setValue(EMBEDDED_TIMELINE_SERVER_REUSE_ENABLED, String.valueOf(enabled));
      return this;
    }

    public Builder withEmbeddedTimelineServerPort(int port) {
      writeConfig.setValue(EMBEDDED_TIMELINE_SERVER_PORT, String.valueOf(port));
      return this;
    }

    public Builder withBulkInsertSortMode(String mode) {
      writeConfig.setValue(BULKINSERT_SORT_MODE, mode);
      return this;
    }

    public Builder withAllowMultiWriteOnSameInstant(boolean allow) {
      writeConfig.setValue(ALLOW_MULTI_WRITE_ON_SAME_INSTANT, String.valueOf(allow));
      return this;
    }

    public Builder withExternalSchemaTrasformation(boolean enabled) {
      writeConfig.setValue(EXTERNAL_RECORD_AND_SCHEMA_TRANSFORMATION, String.valueOf(enabled));
      return this;
    }

    public Builder withMergeDataValidationCheckEnabled(boolean enabled) {
      writeConfig.setValue(MERGE_DATA_VALIDATION_CHECK_ENABLED, String.valueOf(enabled));
      return this;
    }

    public Builder withMergeAllowDuplicateOnInserts(boolean routeInsertsToNewFiles) {
      writeConfig.setValue(MERGE_ALLOW_DUPLICATE_ON_INSERTS, String.valueOf(routeInsertsToNewFiles));
      return this;
    }

    public Builder withSpillableDiskMapType(ExternalSpillableMap.DiskMapType diskMapType) {
      writeConfig.setValue(SPILLABLE_DISK_MAP_TYPE, diskMapType.name());
      return this;
    }

    public Builder withBitcaskDiskMapCompressionEnabled(boolean bitcaskDiskMapCompressionEnabled) {
      writeConfig.setValue(DISK_MAP_BITCASK_COMPRESSION_ENABLED, String.valueOf(bitcaskDiskMapCompressionEnabled));
      return this;
    }

    public Builder withHeartbeatIntervalInMs(Integer heartbeatIntervalInMs) {
      writeConfig.setValue(CLIENT_HEARTBEAT_INTERVAL_IN_MS_PROP, String.valueOf(heartbeatIntervalInMs));
      return this;
    }

    public Builder withHeartbeatTolerableMisses(Integer heartbeatTolerableMisses) {
      writeConfig.setValue(CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES_PROP, String.valueOf(heartbeatTolerableMisses));
      return this;
    }

    public Builder withWriteConcurrencyMode(WriteConcurrencyMode concurrencyMode) {
      writeConfig.setValue(WRITE_CONCURRENCY_MODE_PROP, concurrencyMode.value());
      return this;
    }

    public Builder withWriteMetaKeyPrefixes(String writeMetaKeyPrefixes) {
      writeConfig.setValue(WRITE_META_KEY_PREFIXES_PROP, writeMetaKeyPrefixes);
      return this;
    }

    public Builder withBaseFileFormat(HoodieFileFormat fileFormat) {
      writeConfig.setDefaultValue(HOODIE_BASE_FILE_FORMAT_PROP, fileFormat);
      return this;
    }

    public Builder withProperties(Properties properties) {
      this.writeConfig.getProps().putAll(properties);
      return this;
    }

    protected void setDefaults() {
      // Check for mandatory properties
      writeConfig.setDefaults(HoodieWriteConfig.class.getName());
      // Make sure the props is propagated
      writeConfig.setDefaultOnCondition(
          !isIndexConfigSet, HoodieIndexConfig.newBuilder().withEngineType(engineType).fromProperties(
              writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isStorageConfigSet, HoodieStorageConfig.newBuilder().fromProperties(
          writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isCompactionConfigSet,
          HoodieCompactionConfig.newBuilder().fromProperties(writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isClusteringConfigSet,
          HoodieClusteringConfig.newBuilder().fromProperties(writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isMetricsConfigSet, HoodieMetricsConfig.newBuilder().fromProperties(
          writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isBootstrapConfigSet,
          HoodieBootstrapConfig.newBuilder().fromProperties(writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isMemoryConfigSet, HoodieMemoryConfig.newBuilder().fromProperties(
          writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isViewConfigSet,
          FileSystemViewStorageConfig.newBuilder().fromProperties(writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isConsistencyGuardSet,
          ConsistencyGuardConfig.newBuilder().fromProperties(writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isCallbackConfigSet,
          HoodieWriteCommitCallbackConfig.newBuilder().fromProperties(writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isPayloadConfigSet,
          HoodiePayloadConfig.newBuilder().fromProperties(writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isMetadataConfigSet,
          HoodieMetadataConfig.newBuilder().fromProperties(writeConfig.getProps()).build());
      writeConfig.setDefaultOnCondition(!isLockConfigSet,
          HoodieLockConfig.newBuilder().fromProperties(writeConfig.getProps()).build());

      writeConfig.setDefaultValue(TIMELINE_LAYOUT_VERSION, String.valueOf(TimelineLayoutVersion.CURR_VERSION));
    }

    private void validate() {
      String layoutVersion = writeConfig.getString(TIMELINE_LAYOUT_VERSION);
      // Ensure Layout Version is good
      new TimelineLayoutVersion(Integer.parseInt(layoutVersion));
      Objects.requireNonNull(writeConfig.getString(BASE_PATH_PROP));
      if (writeConfig.getString(WRITE_CONCURRENCY_MODE_PROP)
          .equalsIgnoreCase(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name())) {
        ValidationUtils.checkArgument(writeConfig.getString(HoodieCompactionConfig.FAILED_WRITES_CLEANER_POLICY_PROP)
            != HoodieFailedWritesCleaningPolicy.EAGER.name(), "To enable optimistic concurrency control, set hoodie.cleaner.policy.failed.writes=LAZY");
      }
    }

    public HoodieWriteConfig build() {
      setDefaults();
      validate();
      // Build WriteConfig at the end
      return new HoodieWriteConfig(engineType, writeConfig.getProps());
    }
  }
}

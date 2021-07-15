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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.HoodieWriterUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.embedded.EmbeddedTimelineServerHelper;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.sync.common.AbstractSyncTool;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.exception.HoodieDeltaStreamerException;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.Config;
import org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallback;
import org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallbackConfig;
import org.apache.hudi.utilities.schema.DelegatingSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.schema.SchemaSet;
import org.apache.hudi.utilities.sources.InputBatch;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;
import org.apache.hudi.utilities.transform.Transformer;

import com.codahale.metrics.Timer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Function;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import scala.collection.JavaConversions;

import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_ARCHIVELOG_FOLDER_PROP;
import static org.apache.hudi.config.HoodieClusteringConfig.ASYNC_CLUSTERING_ENABLE_OPT_KEY;
import static org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.CHECKPOINT_KEY;
import static org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.CHECKPOINT_RESET_KEY;
import static org.apache.hudi.config.HoodieClusteringConfig.INLINE_CLUSTERING_PROP;
import static org.apache.hudi.config.HoodieCompactionConfig.INLINE_COMPACT_PROP;
import static org.apache.hudi.config.HoodieWriteConfig.COMBINE_BEFORE_INSERT_PROP;
import static org.apache.hudi.config.HoodieWriteConfig.COMBINE_BEFORE_UPSERT_PROP;
import static org.apache.hudi.config.HoodieWriteConfig.HOODIE_AUTO_COMMIT_PROP;
import static org.apache.hudi.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE;
import static org.apache.hudi.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME;

/**
 * Sync's one batch of data to hoodie table.
 */
public class DeltaSync implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(DeltaSync.class);

  /**
   * Delta Sync Config.
   */
  private final HoodieDeltaStreamer.Config cfg;

  /**
   * Source to pull deltas from.
   */
  private transient SourceFormatAdapter formatAdapter;

  /**
   * User Provided Schema Provider.
   */
  private transient SchemaProvider userProvidedSchemaProvider;

  /**
   * Schema provider that supplies the command for reading the input and writing out the target table.
   */
  private transient SchemaProvider schemaProvider;

  /**
   * Allows transforming source to target table before writing.
   */
  private transient Option<Transformer> transformer;

  /**
   * Extract the key for the target table.
   */
  private KeyGenerator keyGenerator;

  /**
   * Filesystem used.
   */
  private transient FileSystem fs;

  /**
   * Spark context.
   */
  private transient JavaSparkContext jssc;

  /**
   * Spark Session.
   */
  private transient SparkSession sparkSession;

  /**
   * Hive Config.
   */
  private transient Configuration conf;

  /**
   * Bag of properties with source, hoodie client, key generator etc.
   */
  private final TypedProperties props;

  /**
   * Callback when write client is instantiated.
   */
  private transient Function<SparkRDDWriteClient, Boolean> onInitializingHoodieWriteClient;

  /**
   * Timeline with completed commits.
   */
  private transient Option<HoodieTimeline> commitTimelineOpt;

  /**
   * Tracks whether new schema is being seen and creates client accordingly.
   */
  private final SchemaSet processedSchema;

  /**
   * DeltaSync will explicitly manage embedded timeline server so that they can be reused across Write Client
   * instantiations.
   */
  private transient Option<EmbeddedTimelineService> embeddedTimelineService = Option.empty();

  /**
   * Write Client.
   */
  private transient SparkRDDWriteClient writeClient;

  private transient HoodieDeltaStreamerMetrics metrics;

  public DeltaSync(HoodieDeltaStreamer.Config cfg, SparkSession sparkSession, SchemaProvider schemaProvider,
                   TypedProperties props, JavaSparkContext jssc, FileSystem fs, Configuration conf,
                   Function<SparkRDDWriteClient, Boolean> onInitializingHoodieWriteClient) throws IOException {

    this.cfg = cfg;
    this.jssc = jssc;
    this.sparkSession = sparkSession;
    this.fs = fs;
    this.onInitializingHoodieWriteClient = onInitializingHoodieWriteClient;
    this.props = props;
    this.userProvidedSchemaProvider = schemaProvider;
    this.processedSchema = new SchemaSet();

    refreshTimeline();
    // Register User Provided schema first
    registerAvroSchemas(schemaProvider);

    this.transformer = UtilHelpers.createTransformer(cfg.transformerClassNames);
    this.keyGenerator = HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);

    this.metrics = new HoodieDeltaStreamerMetrics(getHoodieClientConfig(this.schemaProvider));

    this.formatAdapter = new SourceFormatAdapter(
        UtilHelpers.createSource(cfg.sourceClassName, props, jssc, sparkSession, schemaProvider, metrics));
    this.conf = conf;
  }

  /**
   * Refresh Timeline.
   *
   * @throws IOException in case of any IOException
   */
  public void refreshTimeline() throws IOException {
    if (fs.exists(new Path(cfg.targetBasePath))) {
      HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setConf(new Configuration(fs.getConf())).setBasePath(cfg.targetBasePath).setPayloadClassName(cfg.payloadClassName).build();
      switch (meta.getTableType()) {
        case COPY_ON_WRITE:
          this.commitTimelineOpt = Option.of(meta.getActiveTimeline().getCommitTimeline().filterCompletedInstants());
          break;
        case MERGE_ON_READ:
          this.commitTimelineOpt = Option.of(meta.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants());
          break;
        default:
          throw new HoodieException("Unsupported table type :" + meta.getTableType());
      }
    } else {
      this.commitTimelineOpt = Option.empty();
      String partitionColumns = HoodieWriterUtils.getPartitionColumns(keyGenerator);

      HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(cfg.tableType)
          .setTableName(cfg.targetTableName)
          .setArchiveLogFolder(HOODIE_ARCHIVELOG_FOLDER_PROP.defaultValue())
          .setPayloadClassName(cfg.payloadClassName)
          .setBaseFileFormat(cfg.baseFileFormat)
          .setPartitionColumns(partitionColumns)
          .setPreCombineField(cfg.sourceOrderingField)
          .initTable(new Configuration(jssc.hadoopConfiguration()),
            cfg.targetBasePath);
    }
  }

  /**
   * Run one round of delta sync and return new compaction instant if one got scheduled.
   */
  public Pair<Option<String>, JavaRDD<WriteStatus>> syncOnce() throws IOException {
    Pair<Option<String>, JavaRDD<WriteStatus>> result = null;
    Timer.Context overallTimerContext = metrics.getOverallTimerContext();

    // Refresh Timeline
    refreshTimeline();

    Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> srcRecordsWithCkpt = readFromSource(commitTimelineOpt);

    if (null != srcRecordsWithCkpt) {
      // this is the first input batch. If schemaProvider not set, use it and register Avro Schema and start
      // compactor
      if (null == writeClient) {
        this.schemaProvider = srcRecordsWithCkpt.getKey();
        // Setup HoodieWriteClient and compaction now that we decided on schema
        setupWriteClient();
      } else {
        Schema newSourceSchema = srcRecordsWithCkpt.getKey().getSourceSchema();
        Schema newTargetSchema = srcRecordsWithCkpt.getKey().getTargetSchema();
        if (!(processedSchema.isSchemaPresent(newSourceSchema))
            || !(processedSchema.isSchemaPresent(newTargetSchema))) {
          LOG.info("Seeing new schema. Source :" + newSourceSchema.toString(true)
              + ", Target :" + newTargetSchema.toString(true));
          // We need to recreate write client with new schema and register them.
          reInitWriteClient(newSourceSchema, newTargetSchema);
          processedSchema.addSchema(newSourceSchema);
          processedSchema.addSchema(newTargetSchema);
        }
      }

      result = writeToSink(srcRecordsWithCkpt.getRight().getRight(),
          srcRecordsWithCkpt.getRight().getLeft(), metrics, overallTimerContext);
    }

    metrics.updateDeltaStreamerSyncMetrics(System.currentTimeMillis());

    // Clear persistent RDDs
    jssc.getPersistentRDDs().values().forEach(JavaRDD::unpersist);
    return result;
  }

  /**
   * Read from Upstream Source and apply transformation if needed.
   *
   * @param commitTimelineOpt Timeline with completed commits
   * @return Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> Input data read from upstream source, consists
   * of schemaProvider, checkpointStr and hoodieRecord
   * @throws Exception in case of any Exception
   */
  public Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> readFromSource(Option<HoodieTimeline> commitTimelineOpt) throws IOException {
    // Retrieve the previous round checkpoints, if any
    Option<String> resumeCheckpointStr = Option.empty();
    if (commitTimelineOpt.isPresent()) {
      // TODO: now not support replace action HUDI-1500
      Option<HoodieInstant> lastCommit = commitTimelineOpt.get()
          .filter(instant -> !instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)).lastInstant();
      if (lastCommit.isPresent()) {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
            .fromBytes(commitTimelineOpt.get().getInstantDetails(lastCommit.get()).get(), HoodieCommitMetadata.class);
        if (cfg.checkpoint != null && (StringUtils.isNullOrEmpty(commitMetadata.getMetadata(CHECKPOINT_RESET_KEY))
                || !cfg.checkpoint.equals(commitMetadata.getMetadata(CHECKPOINT_RESET_KEY)))) {
          resumeCheckpointStr = Option.of(cfg.checkpoint);
        } else if (!StringUtils.isNullOrEmpty(commitMetadata.getMetadata(CHECKPOINT_KEY))) {
          resumeCheckpointStr = Option.of(commitMetadata.getMetadata(CHECKPOINT_KEY));
        } else if (commitMetadata.getOperationType() == WriteOperationType.CLUSTER) {
          // incase of CLUSTER commit, no checkpoint will be available in metadata.
          resumeCheckpointStr = Option.empty();
        } else if (HoodieTimeline.compareTimestamps(HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS,
            HoodieTimeline.LESSER_THAN, lastCommit.get().getTimestamp())) {
          throw new HoodieDeltaStreamerException(
              "Unable to find previous checkpoint. Please double check if this table "
                  + "was indeed built via delta streamer. Last Commit :" + lastCommit + ", Instants :"
                  + commitTimelineOpt.get().getInstants().collect(Collectors.toList()) + ", CommitMetadata="
                  + commitMetadata.toJsonString());
        }
        if (!StringUtils.isNullOrEmpty(commitMetadata.getMetadata(CHECKPOINT_RESET_KEY))) {
          props.remove(KafkaOffsetGen.Config.KAFKA_CHECKPOINT_TYPE.key());
        }
      }
    } else {
      String partitionColumns = HoodieWriterUtils.getPartitionColumns(keyGenerator);
      HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(cfg.tableType)
          .setTableName(cfg.targetTableName)
          .setArchiveLogFolder(HOODIE_ARCHIVELOG_FOLDER_PROP.defaultValue())
          .setPayloadClassName(cfg.payloadClassName)
          .setBaseFileFormat(cfg.baseFileFormat)
          .setPartitionColumns(partitionColumns)
          .initTable(new Configuration(jssc.hadoopConfiguration()), cfg.targetBasePath);
    }

    if (!resumeCheckpointStr.isPresent() && cfg.checkpoint != null) {
      resumeCheckpointStr = Option.of(cfg.checkpoint);
    }
    LOG.info("Checkpoint to resume from : " + resumeCheckpointStr);

    final Option<JavaRDD<GenericRecord>> avroRDDOptional;
    final String checkpointStr;
    final SchemaProvider schemaProvider;
    if (transformer.isPresent()) {
      // Transformation is needed. Fetch New rows in Row Format, apply transformation and then convert them
      // to generic records for writing
      InputBatch<Dataset<Row>> dataAndCheckpoint =
          formatAdapter.fetchNewDataInRowFormat(resumeCheckpointStr, cfg.sourceLimit);

      Option<Dataset<Row>> transformed =
          dataAndCheckpoint.getBatch().map(data -> transformer.get().apply(jssc, sparkSession, data, props));
      checkpointStr = dataAndCheckpoint.getCheckpointForNextBatch();
      if (this.userProvidedSchemaProvider != null && this.userProvidedSchemaProvider.getTargetSchema() != null) {
        // If the target schema is specified through Avro schema,
        // pass in the schema for the Row-to-Avro conversion
        // to avoid nullability mismatch between Avro schema and Row schema
        avroRDDOptional = transformed
            .map(t -> HoodieSparkUtils.createRdd(
                t, this.userProvidedSchemaProvider.getTargetSchema(),
                HOODIE_RECORD_STRUCT_NAME, HOODIE_RECORD_NAMESPACE).toJavaRDD());
        schemaProvider = this.userProvidedSchemaProvider;
      } else {
        // Use Transformed Row's schema if not overridden. If target schema is not specified
        // default to RowBasedSchemaProvider
        schemaProvider =
            transformed
                .map(r -> (SchemaProvider) new DelegatingSchemaProvider(props, jssc,
                    dataAndCheckpoint.getSchemaProvider(),
                    UtilHelpers.createRowBasedSchemaProvider(r.schema(), props, jssc)))
                .orElse(dataAndCheckpoint.getSchemaProvider());
        avroRDDOptional = transformed
            .map(t -> HoodieSparkUtils.createRdd(
                t, HOODIE_RECORD_STRUCT_NAME, HOODIE_RECORD_NAMESPACE).toJavaRDD());
      }
    } else {
      // Pull the data from the source & prepare the write
      InputBatch<JavaRDD<GenericRecord>> dataAndCheckpoint =
          formatAdapter.fetchNewDataInAvroFormat(resumeCheckpointStr, cfg.sourceLimit);
      avroRDDOptional = dataAndCheckpoint.getBatch();
      checkpointStr = dataAndCheckpoint.getCheckpointForNextBatch();
      schemaProvider = dataAndCheckpoint.getSchemaProvider();
    }

    if (Objects.equals(checkpointStr, resumeCheckpointStr.orElse(null))) {
      LOG.info("No new data, source checkpoint has not changed. Nothing to commit. Old checkpoint=("
           + resumeCheckpointStr + "). New Checkpoint=(" + checkpointStr + ")");
      return null;
    }

    if ((!avroRDDOptional.isPresent()) || (avroRDDOptional.get().isEmpty())) {
      LOG.info("No new data, perform empty commit.");
      return Pair.of(schemaProvider, Pair.of(checkpointStr, jssc.emptyRDD()));
    }

    boolean shouldCombine = cfg.filterDupes || cfg.operation.equals(WriteOperationType.UPSERT);
    JavaRDD<GenericRecord> avroRDD = avroRDDOptional.get();
    JavaRDD<HoodieRecord> records = avroRDD.map(gr -> {
      HoodieRecordPayload payload = shouldCombine ? DataSourceUtils.createPayload(cfg.payloadClassName, gr,
          (Comparable) HoodieAvroUtils.getNestedFieldVal(gr, cfg.sourceOrderingField, false))
          : DataSourceUtils.createPayload(cfg.payloadClassName, gr);
      return new HoodieRecord<>(keyGenerator.getKey(gr), payload);
    });

    return Pair.of(schemaProvider, Pair.of(checkpointStr, records));
  }

  /**
   * Perform Hoodie Write. Run Cleaner, schedule compaction and syncs to hive if needed.
   *
   * @param records             Input Records
   * @param checkpointStr       Checkpoint String
   * @param metrics             Metrics
   * @param overallTimerContext Timer Context
   * @return Option Compaction instant if one is scheduled
   */
  private Pair<Option<String>, JavaRDD<WriteStatus>> writeToSink(JavaRDD<HoodieRecord> records, String checkpointStr,
                                                                 HoodieDeltaStreamerMetrics metrics,
                                                                 Timer.Context overallTimerContext) {
    Option<String> scheduledCompactionInstant = Option.empty();
    // filter dupes if needed
    if (cfg.filterDupes) {
      records = DataSourceUtils.dropDuplicates(jssc, records, writeClient.getConfig());
    }

    boolean isEmpty = records.isEmpty();

    // try to start a new commit
    String instantTime = startCommit();
    LOG.info("Starting commit  : " + instantTime);

    JavaRDD<WriteStatus> writeStatusRDD;
    switch (cfg.operation) {
      case INSERT:
        writeStatusRDD = writeClient.insert(records, instantTime);
        break;
      case UPSERT:
        writeStatusRDD = writeClient.upsert(records, instantTime);
        break;
      case BULK_INSERT:
        writeStatusRDD = writeClient.bulkInsert(records, instantTime);
        break;
      default:
        throw new HoodieDeltaStreamerException("Unknown operation : " + cfg.operation);
    }

    long totalErrorRecords = writeStatusRDD.mapToDouble(WriteStatus::getTotalErrorRecords).sum().longValue();
    long totalRecords = writeStatusRDD.mapToDouble(WriteStatus::getTotalRecords).sum().longValue();
    boolean hasErrors = totalErrorRecords > 0;
    long hiveSyncTimeMs = 0;
    long metaSyncTimeMs = 0;
    if (!hasErrors || cfg.commitOnErrors) {
      HashMap<String, String> checkpointCommitMetadata = new HashMap<>();
      checkpointCommitMetadata.put(CHECKPOINT_KEY, checkpointStr);
      if (cfg.checkpoint != null) {
        checkpointCommitMetadata.put(CHECKPOINT_RESET_KEY, cfg.checkpoint);
      }

      if (hasErrors) {
        LOG.warn("Some records failed to be merged but forcing commit since commitOnErrors set. Errors/Total="
            + totalErrorRecords + "/" + totalRecords);
      }

      boolean success = writeClient.commit(instantTime, writeStatusRDD, Option.of(checkpointCommitMetadata));
      if (success) {
        LOG.info("Commit " + instantTime + " successful!");
        this.formatAdapter.getSource().onCommit(checkpointStr);
        // Schedule compaction if needed
        if (cfg.isAsyncCompactionEnabled()) {
          scheduledCompactionInstant = writeClient.scheduleCompaction(Option.empty());
        }

        if (!isEmpty) {
          syncMeta(metrics);
        }
      } else {
        LOG.info("Commit " + instantTime + " failed!");
        throw new HoodieException("Commit " + instantTime + " failed!");
      }
    } else {
      LOG.error("Delta Sync found errors when writing. Errors/Total=" + totalErrorRecords + "/" + totalRecords);
      LOG.error("Printing out the top 100 errors");
      writeStatusRDD.filter(WriteStatus::hasErrors).take(100).forEach(ws -> {
        LOG.error("Global error :", ws.getGlobalError());
        if (ws.getErrors().size() > 0) {
          ws.getErrors().forEach((key, value) -> LOG.trace("Error for key:" + key + " is " + value));
        }
      });
      // Rolling back instant
      writeClient.rollback(instantTime);
      throw new HoodieException("Commit " + instantTime + " failed and rolled-back !");
    }
    long overallTimeMs = overallTimerContext != null ? overallTimerContext.stop() : 0;

    // Send DeltaStreamer Metrics
    metrics.updateDeltaStreamerMetrics(overallTimeMs);
    return Pair.of(scheduledCompactionInstant, writeStatusRDD);
  }

  /**
   * Try to start a new commit.
   * <p>
   * Exception will be thrown if it failed in 2 tries.
   *
   * @return Instant time of the commit
   */
  private String startCommit() {
    final int maxRetries = 2;
    int retryNum = 1;
    RuntimeException lastException = null;
    while (retryNum <= maxRetries) {
      try {
        return writeClient.startCommit();
      } catch (IllegalArgumentException ie) {
        lastException = ie;
        LOG.error("Got error trying to start a new commit. Retrying after sleeping for a sec", ie);
        retryNum++;
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // No-Op
        }
      }
    }
    throw lastException;
  }

  private String getSyncClassShortName(String syncClassName) {
    return syncClassName.substring(syncClassName.lastIndexOf(".") + 1);
  }

  private void syncMeta(HoodieDeltaStreamerMetrics metrics) {
    Set<String> syncClientToolClasses = new HashSet<>(Arrays.asList(cfg.syncClientToolClass.split(",")));
    // for backward compatibility
    if (cfg.enableHiveSync) {
      cfg.enableMetaSync = true;
      syncClientToolClasses.add(HiveSyncTool.class.getName());
      LOG.info("When set --enable-hive-sync will use HiveSyncTool for backward compatibility");
    }
    if (cfg.enableMetaSync) {
      for (String impl : syncClientToolClasses) {
        Timer.Context syncContext = metrics.getMetaSyncTimerContext();
        impl = impl.trim();
        AbstractSyncTool syncTool = null;
        switch (impl) {
          case "org.apache.hudi.hive.HiveSyncTool":
            HiveSyncConfig hiveSyncConfig = DataSourceUtils.buildHiveSyncConfig(props, cfg.targetBasePath, cfg.baseFileFormat);
            LOG.info("Syncing target hoodie table with hive table(" + hiveSyncConfig.tableName + "). Hive metastore URL :"
                + hiveSyncConfig.jdbcUrl + ", basePath :" + cfg.targetBasePath);
            syncTool = new HiveSyncTool(hiveSyncConfig, new HiveConf(conf, HiveConf.class), fs);
            break;
          default:
            FileSystem fs = FSUtils.getFs(cfg.targetBasePath, jssc.hadoopConfiguration());
            Properties properties = new Properties();
            properties.putAll(props);
            properties.put("basePath", cfg.targetBasePath);
            syncTool = (AbstractSyncTool) ReflectionUtils.loadClass(impl, new Class[]{Properties.class, FileSystem.class}, properties, fs);
        }
        syncTool.syncHoodieTable();
        long metaSyncTimeMs = syncContext != null ? syncContext.stop() : 0;
        metrics.updateDeltaStreamerMetaSyncMetrics(getSyncClassShortName(impl), metaSyncTimeMs);
      }
    }
  }

  public void syncHive() {
    HiveSyncConfig hiveSyncConfig = DataSourceUtils.buildHiveSyncConfig(props, cfg.targetBasePath, cfg.baseFileFormat);
    LOG.info("Syncing target hoodie table with hive table(" + hiveSyncConfig.tableName + "). Hive metastore URL :"
        + hiveSyncConfig.jdbcUrl + ", basePath :" + cfg.targetBasePath);
    HiveConf hiveConf = new HiveConf(conf, HiveConf.class);
    LOG.info("Hive Conf => " + hiveConf.getAllProperties().toString());
    LOG.info("Hive Sync Conf => " + hiveSyncConfig.toString());
    new HiveSyncTool(hiveSyncConfig, hiveConf, fs).syncHoodieTable();
  }

  public void syncHive(HiveConf conf) {
    this.conf = conf;
    syncHive();
  }

  /**
   * Note that depending on configs and source-type, schemaProvider could either be eagerly or lazily created.
   * SchemaProvider creation is a precursor to HoodieWriteClient and AsyncCompactor creation. This method takes care of
   * this constraint.
   */
  public void setupWriteClient() throws IOException {
    if ((null != schemaProvider)) {
      Schema sourceSchema = schemaProvider.getSourceSchema();
      Schema targetSchema = schemaProvider.getTargetSchema();
      reInitWriteClient(sourceSchema, targetSchema);
    }
  }

  private void reInitWriteClient(Schema sourceSchema, Schema targetSchema) throws IOException {
    LOG.info("Setting up new Hoodie Write Client");
    registerAvroSchemas(sourceSchema, targetSchema);
    HoodieWriteConfig hoodieCfg = getHoodieClientConfig(targetSchema);
    if (hoodieCfg.isEmbeddedTimelineServerEnabled()) {
      if (!embeddedTimelineService.isPresent()) {
        embeddedTimelineService = EmbeddedTimelineServerHelper.createEmbeddedTimelineService(new HoodieSparkEngineContext(jssc), hoodieCfg);
      } else {
        EmbeddedTimelineServerHelper.updateWriteConfigWithTimelineServer(embeddedTimelineService.get(), hoodieCfg);
      }
    }

    if (null != writeClient) {
      // Close Write client.
      writeClient.close();
    }
    writeClient = new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jssc), hoodieCfg, embeddedTimelineService);
    onInitializingHoodieWriteClient.apply(writeClient);
  }

  /**
   * Helper to construct Write Client config.
   *
   * @param schemaProvider Schema Provider
   */
  private HoodieWriteConfig getHoodieClientConfig(SchemaProvider schemaProvider) {
    return getHoodieClientConfig(schemaProvider != null ? schemaProvider.getTargetSchema() : null);
  }

  /**
   * Helper to construct Write Client config.
   *
   * @param schema Schema
   */
  private HoodieWriteConfig getHoodieClientConfig(Schema schema) {
    final boolean combineBeforeUpsert = true;
    final boolean autoCommit = false;
    HoodieWriteConfig.Builder builder =
        HoodieWriteConfig.newBuilder().withPath(cfg.targetBasePath).combineInput(cfg.filterDupes, combineBeforeUpsert)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder().withPayloadClass(cfg.payloadClassName)
                // Inline compaction is disabled for continuous mode. otherwise enabled for MOR
                .withInlineCompaction(cfg.isInlineCompactionEnabled()).build())
            .withClusteringConfig(HoodieClusteringConfig.newBuilder()
                .withInlineClustering(cfg.isInlineClusteringEnabled())
                .withAsyncClustering(cfg.isAsyncClusteringEnabled()).build())
            .withPayloadConfig(HoodiePayloadConfig.newBuilder().withPayloadOrderingField(cfg.sourceOrderingField)
                .build())
            .forTable(cfg.targetTableName)
            .withAutoCommit(autoCommit).withProps(props);

    if (null != schema) {
      builder = builder.withSchema(schema.toString());
    }
    HoodieWriteConfig config = builder.build();

    // set default value for {@link HoodieWriteCommitKafkaCallbackConfig} if needed.
    if (config.writeCommitCallbackOn() && HoodieWriteCommitKafkaCallback.class.getName().equals(config.getCallbackClass())) {
      HoodieWriteCommitKafkaCallbackConfig.setCallbackKafkaConfigIfNeeded(config);
    }

    // Validate what deltastreamer assumes of write-config to be really safe
    ValidationUtils.checkArgument(config.inlineCompactionEnabled() == cfg.isInlineCompactionEnabled(),
        String.format("%s should be set to %s", INLINE_COMPACT_PROP.key(), cfg.isInlineCompactionEnabled()));
    ValidationUtils.checkArgument(config.inlineClusteringEnabled() == cfg.isInlineClusteringEnabled(),
        String.format("%s should be set to %s", INLINE_CLUSTERING_PROP.key(), cfg.isInlineClusteringEnabled()));
    ValidationUtils.checkArgument(config.isAsyncClusteringEnabled() == cfg.isAsyncClusteringEnabled(),
        String.format("%s should be set to %s", ASYNC_CLUSTERING_ENABLE_OPT_KEY.key(), cfg.isAsyncClusteringEnabled()));
    ValidationUtils.checkArgument(!config.shouldAutoCommit(),
        String.format("%s should be set to %s", HOODIE_AUTO_COMMIT_PROP.key(), autoCommit));
    ValidationUtils.checkArgument(config.shouldCombineBeforeInsert() == cfg.filterDupes,
        String.format("%s should be set to %s", COMBINE_BEFORE_INSERT_PROP.key(), cfg.filterDupes));
    ValidationUtils.checkArgument(config.shouldCombineBeforeUpsert(),
        String.format("%s should be set to %s", COMBINE_BEFORE_UPSERT_PROP.key(), combineBeforeUpsert));

    return config;
  }

  /**
   * Register Avro Schemas.
   *
   * @param schemaProvider Schema Provider
   */
  private void registerAvroSchemas(SchemaProvider schemaProvider) {
    if (null != schemaProvider) {
      registerAvroSchemas(schemaProvider.getSourceSchema(), schemaProvider.getTargetSchema());
    }
  }

  /**
   * Register Avro Schemas.
   *
   * @param sourceSchema Source Schema
   * @param targetSchema Target Schema
   */
  private void registerAvroSchemas(Schema sourceSchema, Schema targetSchema) {
    // register the schemas, so that shuffle does not serialize the full schemas
    if (null != sourceSchema) {
      List<Schema> schemas = new ArrayList<>();
      schemas.add(sourceSchema);
      if (targetSchema != null) {
        schemas.add(targetSchema);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Registering Schema: " + schemas);
      }
      jssc.sc().getConf().registerAvroSchemas(JavaConversions.asScalaBuffer(schemas).toList());
    }
  }

  /**
   * Close all resources.
   */
  public void close() {
    if (null != writeClient) {
      writeClient.close();
      writeClient = null;
    }

    LOG.info("Shutting down embedded timeline server");
    if (embeddedTimelineService.isPresent()) {
      embeddedTimelineService.get().stop();
    }
  }

  public FileSystem getFs() {
    return fs;
  }

  public TypedProperties getProps() {
    return props;
  }

  public Config getCfg() {
    return cfg;
  }

  public Option<HoodieTimeline> getCommitTimelineOpt() {
    return commitTimelineOpt;
  }

  /**
   * Schedule clustering.
   * Called from {@link HoodieDeltaStreamer} when async clustering is enabled.
   *
   * @return Requested clustering instant.
   */
  public Option<String> getClusteringInstantOpt() {
    return writeClient.scheduleClustering(Option.empty());
  }
}

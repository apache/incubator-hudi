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

package org.apache.hudi.integ.testsuite;

import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.integ.testsuite.HoodieTestSuiteJob.HoodieTestSuiteConfig;
import org.apache.hudi.integ.testsuite.dag.nodes.CleanNode;
import org.apache.hudi.integ.testsuite.dag.nodes.DagNode;
import org.apache.hudi.integ.testsuite.dag.nodes.RollbackNode;
import org.apache.hudi.integ.testsuite.dag.nodes.ScheduleCompactNode;
import org.apache.hudi.integ.testsuite.writer.DeltaWriteStats;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * A writer abstraction for the Hudi test suite. This class wraps different implementations of writers used to perform write operations into the target hudi dataset. Current supported writers are
 * {@link HoodieDeltaStreamerWrapper} and {@link SparkRDDWriteClient}.
 */
public class HoodieTestSuiteWriter implements Serializable {

  private static Logger log = LoggerFactory.getLogger(HoodieTestSuiteWriter.class);

  private HoodieDeltaStreamerWrapper deltaStreamerWrapper;
  private HoodieWriteConfig writeConfig;
  private SparkRDDWriteClient writeClient;
  protected HoodieTestSuiteConfig cfg;
  private Option<String> lastCheckpoint;
  private HoodieReadClient hoodieReadClient;
  private Properties props;
  private String schema;
  private transient Configuration configuration;
  private transient JavaSparkContext sparkContext;
  private static Set<String> VALID_DAG_NODES_TO_ALLOW_WRITE_CLIENT_IN_DELTASTREAMER_MODE = new HashSet<>(
      Arrays.asList(RollbackNode.class.getName(), CleanNode.class.getName(), ScheduleCompactNode.class.getName()));
  private static final String GENERATED_DATA_PATH = "generated.data.path";

  public HoodieTestSuiteWriter(JavaSparkContext jsc, Properties props, HoodieTestSuiteConfig cfg, String schema) throws Exception {
    // We ensure that only 1 instance of HoodieWriteClient is instantiated for a HoodieTestSuiteWriter
    // This does not instantiate a HoodieWriteClient until a
    // {@link HoodieDeltaStreamer#commit(HoodieWriteClient, JavaRDD, Option)} is invoked.
    HoodieSparkEngineContext context = new HoodieSparkEngineContext(jsc);
    this.deltaStreamerWrapper = new HoodieDeltaStreamerWrapper(cfg, jsc);
    this.hoodieReadClient = new HoodieReadClient(context, cfg.targetBasePath);
    this.writeConfig = getHoodieClientConfig(cfg, props, schema);
    if (!cfg.useDeltaStreamer) {
      this.writeClient = new SparkRDDWriteClient(context, writeConfig);
    }
    this.cfg = cfg;
    this.configuration = jsc.hadoopConfiguration();
    this.sparkContext = jsc;
    this.props = props;
    this.schema = schema;
  }

  public HoodieWriteConfig getWriteConfig() {
    return this.writeConfig;
  }

  private HoodieWriteConfig getHoodieClientConfig(HoodieTestSuiteConfig cfg, Properties props, String schema) {
    HoodieWriteConfig.Builder builder =
        HoodieWriteConfig.newBuilder().combineInput(true, true).withPath(cfg.targetBasePath)
            .withAutoCommit(false)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder().withPayloadClass(cfg.payloadClassName).build())
            .withPayloadConfig(HoodiePayloadConfig.newBuilder().withPayloadOrderingField(cfg.sourceOrderingField)
                .build())
            .forTable(cfg.targetTableName)
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
            .withProps(props);
    builder = builder.withSchema(schema);
    return builder.build();
  }

  private boolean allowWriteClientAccess(DagNode dagNode) {
    if (VALID_DAG_NODES_TO_ALLOW_WRITE_CLIENT_IN_DELTASTREAMER_MODE.contains(dagNode.getClass().getName())) {
      return true;
    }
    return false;
  }

  public RDD<GenericRecord> getNextBatch() throws Exception {
    Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> nextBatch = fetchSource();
    lastCheckpoint = Option.of(nextBatch.getValue().getLeft());
    JavaRDD<HoodieRecord> inputRDD = nextBatch.getRight().getRight();
    return inputRDD.map(r -> (GenericRecord) r.getData()
        .getInsertValue(new Schema.Parser().parse(schema)).get()).rdd();
  }

  public Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> fetchSource() throws Exception {
    return this.deltaStreamerWrapper.fetchSource();
  }

  public Option<String> startCommit() {
    if (cfg.useDeltaStreamer) {
      return Option.of(HoodieActiveTimeline.createNewInstantTime());
    } else {
      return Option.of(writeClient.startCommit());
    }
  }

  public JavaRDD<WriteStatus> upsert(Option<String> instantTime) throws Exception {
    if (cfg.useDeltaStreamer) {
      return deltaStreamerWrapper.upsert(WriteOperationType.UPSERT);
    } else {
      Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> nextBatch = fetchSource();
      lastCheckpoint = Option.of(nextBatch.getValue().getLeft());
      return writeClient.upsert(nextBatch.getRight().getRight(), instantTime.get());
    }
  }

  public JavaRDD<WriteStatus> insert(Option<String> instantTime) throws Exception {
    if (cfg.useDeltaStreamer) {
      return deltaStreamerWrapper.insert();
    } else {
      Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> nextBatch = fetchSource();
      lastCheckpoint = Option.of(nextBatch.getValue().getLeft());
      return writeClient.insert(nextBatch.getRight().getRight(), instantTime.get());
    }
  }

  public JavaRDD<WriteStatus> insertOverwrite(Option<String> instantTime) throws Exception {
    if(cfg.useDeltaStreamer){
      return deltaStreamerWrapper.insertOverwrite();
    } else {
      Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> nextBatch = fetchSource();
      lastCheckpoint = Option.of(nextBatch.getValue().getLeft());
      return writeClient.insertOverwrite(nextBatch.getRight().getRight(), instantTime.get()).getWriteStatuses();
    }
  }

  public JavaRDD<WriteStatus> insertOverwriteTable(Option<String> instantTime) throws Exception {
    if(cfg.useDeltaStreamer){
      return deltaStreamerWrapper.insertOverwriteTable();
    } else {
      Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> nextBatch = fetchSource();
      lastCheckpoint = Option.of(nextBatch.getValue().getLeft());
      return writeClient.insertOverwriteTable(nextBatch.getRight().getRight(), instantTime.get()).getWriteStatuses();
    }
  }

  public JavaRDD<WriteStatus> bulkInsert(Option<String> instantTime) throws Exception {
    if (cfg.useDeltaStreamer) {
      return deltaStreamerWrapper.bulkInsert();
    } else {
      Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> nextBatch = fetchSource();
      lastCheckpoint = Option.of(nextBatch.getValue().getLeft());
      return writeClient.bulkInsert(nextBatch.getRight().getRight(), instantTime.get());
    }
  }

  public JavaRDD<WriteStatus> compact(Option<String> instantTime) throws Exception {
    if (cfg.useDeltaStreamer) {
      return deltaStreamerWrapper.compact();
    } else {
      if (!instantTime.isPresent()) {
        Option<Pair<String, HoodieCompactionPlan>> compactionPlanPair = Option
            .fromJavaOptional(hoodieReadClient.getPendingCompactions()
                .stream().findFirst());
        if (compactionPlanPair.isPresent()) {
          instantTime = Option.of(compactionPlanPair.get().getLeft());
        }
      }
      if (instantTime.isPresent()) {
        return (JavaRDD<WriteStatus>) writeClient.compact(instantTime.get());
      } else {
        return null;
      }
    }
  }

  public void inlineClustering() {
    if (!cfg.useDeltaStreamer) {
      Option<String> clusteringInstantOpt = writeClient.scheduleClustering(Option.empty());
      clusteringInstantOpt.ifPresent(clusteringInstant -> {
        // inline cluster should auto commit as the user is never given control
        log.warn("Clustering instant :: " + clusteringInstant);
        writeClient.cluster(clusteringInstant, true);
      });
    } else {
      // TODO: fix clustering to be done async https://issues.apache.org/jira/browse/HUDI-1590
      throw new IllegalArgumentException("Clustering cannot be triggered with deltastreamer");
    }
  }

  public Option<String> scheduleCompaction(Option<Map<String, String>> previousCommitExtraMetadata) throws
      Exception {
    if (!cfg.useDeltaStreamer) {
      deltaStreamerWrapper.scheduleCompact();
      return Option.empty();
    } else {
      return writeClient.scheduleCompaction(previousCommitExtraMetadata);
    }
  }

  public void commit(JavaRDD<WriteStatus> records, JavaRDD<DeltaWriteStats> generatedDataStats,
      Option<String> instantTime) {
    if (!cfg.useDeltaStreamer) {
      Map<String, String> extraMetadata = new HashMap<>();
      /** Store the checkpoint in the commit metadata just like
       * {@link HoodieDeltaStreamer#commit(SparkRDDWriteClient, JavaRDD, Option)} **/
      extraMetadata.put(HoodieDeltaStreamerWrapper.CHECKPOINT_KEY, lastCheckpoint.get());
      if (generatedDataStats != null) {
        // Just stores the path where this batch of data is generated to
        extraMetadata.put(GENERATED_DATA_PATH, generatedDataStats.map(s -> s.getFilePath()).collect().get(0));
      }
      writeClient.commit(instantTime.get(), records, Option.of(extraMetadata));
    }
  }

  public SparkRDDWriteClient getWriteClient(DagNode dagNode) throws IllegalAccessException {
    if (cfg.useDeltaStreamer & !allowWriteClientAccess(dagNode)) {
      throw new IllegalAccessException("cannot access write client when testing in deltastreamer mode");
    }
    synchronized (this) {
      if (writeClient == null) {
        this.writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(this.sparkContext), getHoodieClientConfig(cfg, props, schema));
      }
    }
    return writeClient;
  }

  public HoodieDeltaStreamerWrapper getDeltaStreamerWrapper() {
    return deltaStreamerWrapper;
  }

  public HoodieTestSuiteConfig getCfg() {
    return cfg;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public JavaSparkContext getSparkContext() {
    return sparkContext;
  }

  public Option<String> getLastCheckpoint() {
    return lastCheckpoint;
  }

  public Properties getProps() {
    return props;
  }

  public String getSchema() {
    return schema;
  }
}

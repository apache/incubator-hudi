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

package org.apache.hudi.metrics;

import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class HoodieFlinkMetrics {

  private static final Logger LOG = LogManager.getLogger(HoodieFlinkMetrics.class);
  // Some histograms
  public String bucketFlushHistogramName;
  public String deltaBucketFlushHistogramName;
  public String compactionHistogramName;

  private final String tableName;
  private final MetricGroup metricGroup;

  public HoodieFlinkMetrics(String tableName, MetricGroup metricGroup) {
    this.tableName = tableName;
    this.metricGroup = metricGroup;

    this.bucketFlushHistogramName = getMetricsName("histogram", "bucket_flush_cost");
    this.deltaBucketFlushHistogramName = getMetricsName("histogram", "delta_bucket_flush_cost");
    this.compactionHistogramName = getMetricsName("histogram", "compaction_cost");
  }

  String getMetricsName(String action, String metric) {
    return String.format("%s.%s.%s", tableName, action, metric);
  }

  public Histogram getBucketFlushHistogram() {
    return metricGroup.histogram(bucketFlushHistogramName, createDropwizardHistogramWrapper());
  }

  public Histogram getDeltaBucketFlushHistogram() {
    return metricGroup.histogram(deltaBucketFlushHistogramName, createDropwizardHistogramWrapper());
  }

  public Histogram getCompactionHistogram() {
    return metricGroup.histogram(compactionHistogramName, createDropwizardHistogramWrapper());
  }

  public void updateCommitMetrics(long commitEpochTimeInMs, long durationInMs, HoodieCommitMetadata metadata,
                                  String actionType) {
    updateCommitTimingMetrics(commitEpochTimeInMs, durationInMs, metadata, actionType);
    long totalPartitionsWritten = metadata.fetchTotalPartitionsWritten();
    long totalFilesInsert = metadata.fetchTotalFilesInsert();
    long totalFilesUpdate = metadata.fetchTotalFilesUpdated();
    long totalRecordsWritten = metadata.fetchTotalRecordsWritten();
    long totalUpdateRecordsWritten = metadata.fetchTotalUpdateRecordsWritten();
    long totalInsertRecordsWritten = metadata.fetchTotalInsertRecordsWritten();
    long totalBytesWritten = metadata.fetchTotalBytesWritten();
    long totalTimeTakenByScanner = metadata.getTotalScanTime();
    long totalTimeTakenForInsert = metadata.getTotalCreateTime();
    long totalTimeTakenForUpsert = metadata.getTotalUpsertTime();
    long totalCompactedRecordsUpdated = metadata.getTotalCompactedRecordsUpdated();
    long totalLogFilesCompacted = metadata.getTotalLogFilesCompacted();
    long totalLogFilesSize = metadata.getTotalLogFilesSize();
    metricGroup.gauge(getMetricsName(actionType, "totalPartitionsWritten"), new FlinkGauge(totalPartitionsWritten));
    metricGroup.gauge(getMetricsName(actionType, "totalFilesInsert"), new FlinkGauge(totalFilesInsert));
    metricGroup.gauge(getMetricsName(actionType, "totalFilesUpdate"), new FlinkGauge(totalFilesUpdate));
    metricGroup.gauge(getMetricsName(actionType, "totalRecordsWritten"), new FlinkGauge(totalRecordsWritten));
    metricGroup.gauge(getMetricsName(actionType, "totalUpdateRecordsWritten"), new FlinkGauge(totalUpdateRecordsWritten));
    metricGroup.gauge(getMetricsName(actionType, "totalInsertRecordsWritten"), new FlinkGauge(totalInsertRecordsWritten));
    metricGroup.gauge(getMetricsName(actionType, "totalBytesWritten"), new FlinkGauge(totalBytesWritten));
    metricGroup.gauge(getMetricsName(actionType, "totalScanTime"), new FlinkGauge(totalTimeTakenByScanner));
    metricGroup.gauge(getMetricsName(actionType, "totalCreateTime"), new FlinkGauge(totalTimeTakenForInsert));
    metricGroup.gauge(getMetricsName(actionType, "totalUpsertTime"), new FlinkGauge(totalTimeTakenForUpsert));
    metricGroup.gauge(getMetricsName(actionType, "totalCompactedRecordsUpdated"), new FlinkGauge(totalCompactedRecordsUpdated));
    metricGroup.gauge(getMetricsName(actionType, "totalLogFilesCompacted"), new FlinkGauge(totalLogFilesCompacted));
    metricGroup.gauge(getMetricsName(actionType, "totalLogFilesSize"), new FlinkGauge(totalLogFilesSize));
  }

  private void updateCommitTimingMetrics(long commitEpochTimeInMs, long durationInMs, HoodieCommitMetadata metadata,
                                         String actionType) {
    Pair<Option<Long>, Option<Long>> eventTimePairMinMax = metadata.getMinAndMaxEventTime();
    if (eventTimePairMinMax.getLeft().isPresent()) {
      long commitLatencyInMs = commitEpochTimeInMs + durationInMs - eventTimePairMinMax.getLeft().get();
      metricGroup.gauge(getMetricsName(actionType, "commitLatencyInMs"), new FlinkGauge(commitLatencyInMs));
    }
    if (eventTimePairMinMax.getRight().isPresent()) {
      long commitFreshnessInMs = commitEpochTimeInMs + durationInMs - eventTimePairMinMax.getRight().get();
      metricGroup.gauge(getMetricsName(actionType, "commitFreshnessInMs"), new FlinkGauge(commitFreshnessInMs));
    }
    metricGroup.gauge(getMetricsName(actionType, "commitTime"), new FlinkGauge(commitEpochTimeInMs));
    metricGroup.gauge(getMetricsName(actionType, "duration"), new FlinkGauge(durationInMs));
  }

  public void updateRollbackMetrics(long durationInMs, long numFilesDeleted) {
    LOG.info(
        String.format("Sending rollback metrics (duration=%d, numFilesDeleted=%d)", durationInMs, numFilesDeleted));
    metricGroup.gauge(getMetricsName("rollback", "duration"), new FlinkGauge(durationInMs));
    metricGroup.gauge(getMetricsName("rollback", "numFilesDeleted"), new FlinkGauge(numFilesDeleted));
  }

  public void updateCleanMetrics(long durationInMs, int numFilesDeleted) {
    LOG.info(
        String.format("Sending clean metrics (duration=%d, numFilesDeleted=%d)", durationInMs, numFilesDeleted));
    metricGroup.gauge(getMetricsName("clean", "duration"), new FlinkGauge(durationInMs));
    metricGroup.gauge(getMetricsName("clean", "numFilesDeleted"), new FlinkGauge(numFilesDeleted));
  }

  public void updateFinalizeWriteMetrics(long durationInMs, long numFilesFinalized) {
    LOG.info(String.format("Sending finalize write metrics (duration=%d, numFilesFinalized=%d)", durationInMs,
        numFilesFinalized));
    metricGroup.gauge(getMetricsName("finalize", "duration"), new FlinkGauge(durationInMs));
    metricGroup.gauge(getMetricsName("finalize", "numFilesFinalized"), new FlinkGauge(numFilesFinalized));
  }

  private DropwizardHistogramWrapper createDropwizardHistogramWrapper() {
    return new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500)));
  }

  /**
   * Gauge for Flink.
   */
  private static class FlinkGauge implements Gauge<Long> {

    private final long value;

    FlinkGauge(long value) {
      this.value = value;
    }

    @Override
    public Long getValue() {
      return value;
    }
  }
}

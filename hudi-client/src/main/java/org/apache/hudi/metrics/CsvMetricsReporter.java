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

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;

import java.io.Closeable;
import java.io.File;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


/**
 * Implementation of CSV reporter, which save metrics to CSV files.
 */
public class CsvMetricsReporter extends MetricsReporter {

  private static Logger logger = LogManager.getLogger(CsvMetricsReporter.class);
  private final MetricRegistry registry;
  private final CsvReporter csvReporter;

  private int pollPeriod;
  private TimeUnit pollUnit;
  private String pollDir;

  public CsvMetricsReporter(HoodieWriteConfig config, MetricRegistry registry) {
    this.registry = registry;

    this.pollPeriod = config.getCsvKeyPeriod();
    this.pollUnit = TimeUnit.valueOf(config.getCsvKeyUnit().toUpperCase(Locale.ROOT));
    this.pollDir = config.getCsvKeyDir();
    this.csvReporter = createCsvReport();
  }

  @Override
  public void start() {
    if (csvReporter != null) {
      csvReporter.start(pollPeriod, pollUnit);
    } else {
      logger.error("Cannot start as the graphiteReporter is null.");
    }
  }

  @Override
  public void report() {
    if (csvReporter != null) {
      csvReporter.report();
    } else {
      logger.error("Cannot report metrics as the graphiteReporter is null.");
    }
  }

  @Override
  public Closeable getReporter() {
    return csvReporter;
  }

  private CsvReporter createCsvReport() {
    return CsvReporter.forRegistry(registry)
        .formatFor(Locale.US)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .convertRatesTo(TimeUnit.SECONDS)
        .build(new File(pollDir));
  }
}

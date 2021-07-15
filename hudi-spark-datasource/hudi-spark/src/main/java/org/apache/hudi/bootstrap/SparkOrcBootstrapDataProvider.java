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

package org.apache.hudi.bootstrap;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.client.bootstrap.FullRecordBootstrapDataProvider;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.bootstrap.FileStatusUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.KeyGenerator;

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.List;

/**
 * Spark Data frame based bootstrap input provider.
 */
public class SparkOrcBootstrapDataProvider extends FullRecordBootstrapDataProvider<JavaRDD<HoodieRecord>> {
  private final transient SparkSession sparkSession;

  public SparkOrcBootstrapDataProvider(TypedProperties props,
                                             HoodieSparkEngineContext context) {
    super(props, context);
    this.sparkSession = SparkSession.builder().config(context.getJavaSparkContext().getConf()).getOrCreate();
  }

  @Override
  public JavaRDD<HoodieRecord> generateInputRecords(String tableName, String sourceBasePath,
                                                      List<Pair<String, List<HoodieFileStatus>>> partitionPathsWithFiles) {
    String[] filePaths = partitionPathsWithFiles.stream().map(Pair::getValue)
                .flatMap(f -> f.stream().map(fs -> FileStatusUtils.toPath(fs.getPath()).toString()))
                .toArray(String[]::new);

    Dataset inputDataset = sparkSession.read().orc(filePaths);
    try {
      KeyGenerator keyGenerator = HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);
      String structName = tableName + "_record";
      String namespace = "hoodie." + tableName;
      RDD<GenericRecord> genericRecords = HoodieSparkUtils.createRdd(inputDataset, structName, namespace);
      return genericRecords.toJavaRDD().map(gr -> {
        String orderingVal = HoodieAvroUtils.getNestedFieldValAsString(
                  gr, props.getString("hoodie.datasource.write.precombine.field"), false);
        try {
          return DataSourceUtils.createHoodieRecord(gr, orderingVal, keyGenerator.getKey(gr),
                 props.getString("hoodie.datasource.write.payload.class"));
        } catch (IOException ioe) {
          throw new HoodieIOException(ioe.getMessage(), ioe);
        }
      });
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }
}
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

package org.apache.hudi.examples.java;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.examples.datagen.HoodieExampleDataGenerator;
import org.apache.hudi.index.HoodieIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Simple examples of #{@link HoodieJavaWriteClient}.
 *
 * Usage: HoodieJavaWriteClientExample <tablePath> <tableName>
 * <tablePath> and <tableName> describe root path of hudi and table name
 * for example, `HoodieJavaWriteClientExample file:///tmp/hoodie/sample-table hoodie_rt`
 */
public class HoodieJavaWriteClientExample {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieJavaWriteClientExample.class);

  private static String tableType = HoodieTableType.COPY_ON_WRITE.name();

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: HoodieJavaWriteClientExample <tablePath> <tableName>");
      System.exit(1);
    }
    String tablePath = args[0];
    String tableName = args[1];

    // Generator of some records to be loaded in.
    HoodieExampleDataGenerator<HoodieAvroPayload> dataGen = new HoodieExampleDataGenerator<>();

    Configuration hadoopConf = new Configuration();
    // initialize the table, if not done already
    Path path = new Path(tablePath);
    FileSystem fs = FSUtils.getFs(tablePath, hadoopConf);
    if (!fs.exists(path)) {
      HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(tableType)
        .setTableName(tableName)
        .setPayloadClassName(HoodieAvroPayload.class.getName())
        .initTable(hadoopConf, tablePath);
    }

    // Create the write client to write some records in
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
        .withSchema(HoodieExampleDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withDeleteParallelism(2).forTable(tableName)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(20, 30).build()).build();
    HoodieJavaWriteClient<HoodieAvroPayload> client =
        new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(hadoopConf), cfg);

    // inserts
    String newCommitTime = client.startCommit();
    LOG.info("Starting commit " + newCommitTime);

    List<HoodieRecord<HoodieAvroPayload>> records = dataGen.generateInserts(newCommitTime, 10);
    List<HoodieRecord<HoodieAvroPayload>> recordsSoFar = new ArrayList<>(records);
    List<HoodieRecord<HoodieAvroPayload>> writeRecords =
        recordsSoFar.stream().map(HoodieRecord::new).collect(Collectors.toList());
    client.upsert(writeRecords, newCommitTime);

    // updates
    newCommitTime = client.startCommit();
    LOG.info("Starting commit " + newCommitTime);
    List<HoodieRecord<HoodieAvroPayload>> toBeUpdated = dataGen.generateUpdates(newCommitTime, 2);
    records.addAll(toBeUpdated);
    recordsSoFar.addAll(toBeUpdated);
    writeRecords =
        recordsSoFar.stream().map(HoodieRecord::new).collect(Collectors.toList());
    client.upsert(writeRecords, newCommitTime);

    // Delete
    newCommitTime = client.startCommit();
    LOG.info("Starting commit " + newCommitTime);
    // just delete half of the records
    int numToDelete = recordsSoFar.size() / 2;
    List<HoodieKey> toBeDeleted =
        recordsSoFar.stream().map(HoodieRecord::getKey).limit(numToDelete).collect(Collectors.toList());
    client.delete(toBeDeleted, newCommitTime);

    client.close();
  }
}

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

package org.apache.hudi.io.storage;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hudi.avro.model.HoodieFileInfo;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.avro.model.HoodieRangeIndexInfo;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieFileWriterFactory}.
 */
public class TestHoodieFileWriterFactory extends HoodieClientTestBase {

  @Test
  public void testGetFileWriter() throws IOException {
    // parquet file format.
    final String instantTime = "100";
    final Path parquetPath = new Path(basePath + "/partition/path/f1_1-0-1_000.parquet");
    final HoodieWriteConfig cfg = getConfig();
    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    SparkTaskContextSupplier supplier = new SparkTaskContextSupplier();
    HoodieFileWriter<IndexedRecord> parquetWriter = HoodieFileWriterFactory.getFileWriter(instantTime,
        parquetPath, table, cfg, HoodieTestDataGenerator.AVRO_SCHEMA, supplier);
    assertTrue(parquetWriter instanceof HoodieParquetWriter);

    // hfile format.
    final Path hfilePath = new Path(basePath + "/partition/path/f1_1-0-1_000.hfile");
    HoodieFileWriter<IndexedRecord> hfileWriter = HoodieFileWriterFactory.getFileWriter(instantTime,
        hfilePath, table, cfg, HoodieTestDataGenerator.AVRO_SCHEMA, supplier);
    assertTrue(hfileWriter instanceof HoodieHFileWriter);

    // orc file format.
    final Path orcPath = new Path(basePath + "/partition/path/f1_1-0-1_000.orc");
    HoodieFileWriter<IndexedRecord> orcFileWriter = HoodieFileWriterFactory.getFileWriter(instantTime,
        orcPath, table, cfg, HoodieTestDataGenerator.AVRO_SCHEMA, supplier);
    assertTrue(orcFileWriter instanceof HoodieOrcWriter);

    // other file format exception.
    final Path logPath = new Path(basePath + "/partition/path/f.b51192a8-574b-4a85-b246-bcfec03ac8bf_100.log.2_1-0-1");
    final Throwable thrown = assertThrows(UnsupportedOperationException.class, () -> {
      HoodieFileWriter<IndexedRecord> logWriter = HoodieFileWriterFactory.getFileWriter(instantTime, logPath,
          table, cfg, HoodieTestDataGenerator.AVRO_SCHEMA, supplier);
    }, "should fail since log storage writer is not supported yet.");
    assertTrue(thrown.getMessage().contains("format not supported yet."));
  }
  
  // key: full file path (/tmp/.../partition0000/file-000.parquet, value: column range 
  @Test
  public void testPerformanceRangeKeyPartitionFile() throws IOException {
    final String instantTime = "100";
    final HoodieWriteConfig cfg = getConfig();
    final Path hfilePath = new Path(basePath + "/hfile_partition/f1_1-0-1_000.hfile");
    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    HoodieHFileWriter<HoodieRecordPayload, IndexedRecord> hfileWriter = (HoodieHFileWriter<HoodieRecordPayload, IndexedRecord>) HoodieFileWriterFactory.getFileWriter(instantTime,
        hfilePath, table, cfg, HoodieMetadataRecord.SCHEMA$, supplier);
    Random random = new Random();
    
    int numPartitions = 1000;
    int avgFilesPerPartition = 10;

    long startTime = System.currentTimeMillis();
    List<String> partitions = new ArrayList<>();
    for (int i = 0; i < numPartitions; i++) {
      String partitionPath = "partition-" + String.format("%010d", i);
      partitions.add(partitionPath);
      for (int j = 0; j < avgFilesPerPartition; j++) {
        String filePath = "file-" + String.format("%010d", j) + "_1-0-1_000.parquet";
        int max = random.nextInt();
        if (max < 0) {
          max = -max;
        }
        int min = random.nextInt(max);

        HoodieKey key = new HoodieKey(partitionPath + filePath, partitionPath);
        GenericRecord rec = new GenericData.Record(HoodieMetadataRecord.SCHEMA$);
        rec.put("key", key.getRecordKey());
        rec.put("type", 2);
        rec.put("rangeIndexMetadata", HoodieRangeIndexInfo.newBuilder().setMax("" + max).setMin("" + min).setIsDeleted(false).build());
        hfileWriter.writeAvro(key.getRecordKey(), rec);
      }
    }
    
    hfileWriter.close();
    long durationInMs = System.currentTimeMillis() - startTime;
    System.out.println("Time taken to generate & write: " + durationInMs + " ms. file path: " + hfilePath
        + " File size: " + FSUtils.getFileSize(metaClient.getFs(), hfilePath));
    
    CacheConfig cacheConfig = new CacheConfig(hadoopConf);
    cacheConfig.setCacheDataInL1(false);
    HoodieHFileReader reader = new HoodieHFileReader(hadoopConf, hfilePath, cacheConfig);
    long duration  = 0;
    int numRuns = 1000;
    long numRecordsInRange = 0;
    for (int i = 0; i < numRuns; i++) {
      int partitionPicked = Math.max(0, partitions.size() - 30);
      long start = System.currentTimeMillis();
      Map<String, GenericRecord> records = reader.getRecordsInRange(partitions.get(partitionPicked), partitions.get(partitions.size() - 1));
      duration += (System.currentTimeMillis() - start);
      numRecordsInRange += records.size();
    }
    double avgDuration = duration / (double) numRuns;
    double avgRecordsFetched = numRecordsInRange / (double) numRuns;
    System.out.println("Average time taken to lookup a range: " + avgDuration + "ms. Avg number records: " + avgRecordsFetched);
  }

  // key: partition (partition0000), value: map (filePath -> column range)
  @Test
  public void testPerformanceRangeKeyPartition() throws IOException {
    final String instantTime = "100";
    final HoodieWriteConfig cfg = getConfig();
    final Path hfilePath = new Path(basePath + "/hfile_partition/f1_1-0-1_000.hfile");
    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    HoodieHFileWriter<HoodieRecordPayload, IndexedRecord> hfileWriter = (HoodieHFileWriter<HoodieRecordPayload, IndexedRecord>) HoodieFileWriterFactory.getFileWriter(instantTime,
        hfilePath, table, cfg, HoodieMetadataRecord.SCHEMA$, supplier);
    Random random = new Random();

    int numPartitions = 1;
    int avgFilesPerPartition = 10;

    long startTime = System.currentTimeMillis();
    List<String> partitions = new ArrayList<>();
    for (int i = 0; i < numPartitions; i++) {
      String partitionPath = "partition-" + String.format("%010d", i);
      partitions.add(partitionPath);
      Map<String, HoodieRangeIndexInfo> fileToRangeInfo = new HashMap<>();
      for (int j = 0; j < avgFilesPerPartition; j++) {
        String filePath = "file-" + String.format("%010d", j) + "_1-0-1_000.parquet";
        int max = random.nextInt();
        if (max < 0) {
          max = -max;
        }
        int min = random.nextInt(max);
        fileToRangeInfo.put(filePath, HoodieRangeIndexInfo.newBuilder().setMax("" + max).setMin("" + min).setIsDeleted(false).build());
      }

      HoodieKey key = new HoodieKey(partitionPath, partitionPath);
      GenericRecord rec = new GenericData.Record(HoodieMetadataRecord.SCHEMA$);
      rec.put("key", key.getRecordKey());
      rec.put("type", 2);
      rec.put("partitionRangeIndexMetadata", fileToRangeInfo);
      hfileWriter.writeAvro(key.getRecordKey(), rec);
    }

    hfileWriter.close();
    long durationInMs = System.currentTimeMillis() - startTime;
    System.out.println("Time taken to generate & write: " + durationInMs + " ms. file path: " + hfilePath
        + " File size: " + FSUtils.getFileSize(metaClient.getFs(), hfilePath));
    
    CacheConfig cacheConfig = new CacheConfig(hadoopConf);
    cacheConfig.setCacheDataInL1(false);
    HoodieHFileReader reader = new HoodieHFileReader(hadoopConf, hfilePath, cacheConfig);
    GenericRecord record = (GenericRecord) reader.getRecordByKey(partitions.get(0), reader.getSchema()).get();
    assertEquals(avgFilesPerPartition, ((Map<String, HoodieRangeIndexInfo> )record.get("partitionRangeIndexMetadata")).size());
    long duration  = 0;
    int numRuns = 1000;
    long numRecordsInRange = 0;
    for (int i = 0; i < numRuns; i++) {
      int partitionPicked = Math.max(0, partitions.size() - 30);
      long start = System.currentTimeMillis();
      Map<String, GenericRecord> records = (Map<String, GenericRecord>) 
          reader.getRecordsInRange(partitions.get(partitionPicked), partitions.get(partitions.size() - 1));
      numRecordsInRange += records.size();
      duration += (System.currentTimeMillis() - start);
    }
    double avgDuration = duration / (double) numRuns;
    double avgRecordsFetched = numRecordsInRange / (double) numRuns;
    System.out.println("Average time taken to lookup a range: " + avgDuration + "ms. Avg number records: " + avgRecordsFetched);
  }

  // key: column range (400), value: list (filePaths containing that range)
  @Test
  public void testPerformanceRangeKeyColumnRange() throws IOException {
    final String instantTime = "100";
    final HoodieWriteConfig cfg = getConfig();
    final Path hfilePath = new Path(basePath + "/hfile_partition/f1_1-0-1_000.hfile");
    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    HoodieHFileWriter<HoodieRecordPayload, IndexedRecord> hfileWriter = (HoodieHFileWriter<HoodieRecordPayload, IndexedRecord>) HoodieFileWriterFactory.getFileWriter(instantTime,
        hfilePath, table, cfg, HoodieMetadataRecord.SCHEMA$, supplier);
    Random random = new Random();

    int numKeys = 300;
    int minFilesPerRange = 10;
    int maxFilesPerRange = 30;

    int currentStart = 0;
    long startTime = System.currentTimeMillis();
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < numKeys; i++) {
      String key = "ID-" + String.format("%010d", currentStart);
      int numFiles = minFilesPerRange + random.nextInt(maxFilesPerRange - minFilesPerRange);
      List<HoodieFileInfo> files = new ArrayList<>();
      for (int j = 0; j < numFiles; j++) {
        files.add(HoodieFileInfo.newBuilder().setFilePath(UUID.randomUUID().toString()).setIsDeleted(false).build());
      }
      GenericRecord rec = new GenericData.Record(HoodieMetadataRecord.SCHEMA$);
      rec.put("key", key);
      rec.put("type", 4);
      rec.put("rangeToFileInfo", files);
      hfileWriter.writeAvro(key, rec);
      if (i % 10 == 0) {
        keys.add(key);
      }
      currentStart += 1000;
    }

    hfileWriter.close();
    long durationInMs = System.currentTimeMillis() - startTime;
    System.out.println("Time taken to generate & write: " + durationInMs + " ms. file path: " + hfilePath 
        + " File size: " + FSUtils.getFileSize(metaClient.getFs(), hfilePath));

    CacheConfig cacheConfig = new CacheConfig(hadoopConf);
    cacheConfig.setCacheDataInL1(false);
    HoodieHFileReader reader = new HoodieHFileReader(hadoopConf, hfilePath, cacheConfig);
    long duration  = 0;
    int numRuns = 1000;
    long numRecords = 0;
    for (int i = 0; i < numRuns; i++) {
      int minKeyPicked = random.nextInt(keys.size() / 2);
      int maxKeyPicked = minKeyPicked + random.nextInt(keys.size() / 2 - 1);
      long start = System.currentTimeMillis();
      Map<String, GenericRecord> records = (Map<String, GenericRecord>)
          reader.getRecordsInRange(keys.get(minKeyPicked), keys.get(maxKeyPicked));
      numRecords += records.size();
      duration += (System.currentTimeMillis() - start);
    }
    double avgDuration = duration / (double) numRuns;
    double avgRecordsFetched = numRecords / (double) numRuns;
    System.out.println("Average time taken to lookup a range: " + avgDuration + "ms. Avg number records: " + avgRecordsFetched);
  }

  // measure time for single key lookup
  @Test
  public void testPerformanceSingleKey() throws IOException {
    final String instantTime = "100";
    final HoodieWriteConfig cfg = getConfig();
    final Path hfilePath = new Path(basePath + "/hfile_partition/f1_1-0-1_000.hfile");
    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    HoodieHFileWriter<HoodieRecordPayload, IndexedRecord> hfileWriter = (HoodieHFileWriter<HoodieRecordPayload, IndexedRecord>) HoodieFileWriterFactory.getFileWriter(instantTime,
        hfilePath, table, cfg, HoodieMetadataRecord.SCHEMA$, supplier);
    Random random = new Random();

    int numPartitions = 10000;
    int avgFilesPerPartition = 10;

    long startTime = System.currentTimeMillis();
    List<String> allKeys = new ArrayList<>();
    for (int i = 0; i < numPartitions; i++) {
      String partitionPath = "partition-" + String.format("%010d", i);
      for (int j = 0; j < avgFilesPerPartition; j++) {
        String filePath = "file-" + String.format("%010d", j) + "_1-0-1_000.parquet";
        int max = random.nextInt() + 1;
        if (max < 0) {
          max = -max;
        }
        int min = random.nextInt(max);

        HoodieKey key = new HoodieKey(partitionPath + filePath, partitionPath);
        GenericRecord rec = new GenericData.Record(HoodieMetadataRecord.SCHEMA$);
        if (j == 0) {
          allKeys.add(key.getRecordKey());
        }
        rec.put("key", key.getRecordKey());
        rec.put("type", 2);
        rec.put("rangeIndexMetadata", HoodieRangeIndexInfo.newBuilder().setMax("" + max).setMin("" + min).setIsDeleted(false).build());
        hfileWriter.writeAvro(key.getRecordKey(), rec);
      }
    }

    hfileWriter.close();
    long durationInMs = System.currentTimeMillis() - startTime;
    System.out.println("Time taken to write: " + durationInMs + " ms");

    CacheConfig cacheConfig = new CacheConfig(hadoopConf);
    cacheConfig.setCacheDataInL1(false);
    HoodieHFileReader reader = new HoodieHFileReader(hadoopConf, hfilePath, cacheConfig);
    long duration  = 0;
    int numRuns = 1000;
    Schema schema = reader.getSchema();
    for (int i = 0; i < numRuns; i++) {
      String keyPicked = allKeys.get(random.nextInt(allKeys.size() - 1));
      long start = System.currentTimeMillis();
      GenericRecord record = (GenericRecord) reader.getRecordByKey(keyPicked, schema).get();
      duration += (System.currentTimeMillis() - start);
    }
    double avgDuration = duration / (double) numRuns;
    System.out.println("Average time taken to lookup a key: " + avgDuration + "ms.");
  }
}

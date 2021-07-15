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

package org.apache.hudi.utilities.functional;

import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.testutils.FunctionalTestHarness;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.utilities.HDFSParquetImporter;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
public class TestHDFSParquetImporter extends FunctionalTestHarness implements Serializable {

  private String basePath;
  private transient Path hoodieFolder;
  private transient Path srcFolder;
  private transient List<GenericRecord> insertData;

  @BeforeEach
  public void init() throws IOException, ParseException {
    basePath = (new Path(dfsBasePath(), Thread.currentThread().getStackTrace()[1].getMethodName())).toString();

    // Hoodie root folder.
    hoodieFolder = new Path(basePath, "testTarget");

    // Create generic records.
    srcFolder = new Path(basePath, "testSrc");
    insertData = createInsertRecords(srcFolder);
  }

  @AfterEach
  public void clean() throws IOException {
    dfs().delete(new Path(basePath), true);
  }

  /**
   * Test successful data import with retries.
   */
  @Test
  public void testImportWithRetries() throws Exception {
    // Create schema file.
    String schemaFile = new Path(basePath, "file.schema").toString();

    HDFSParquetImporter.Config cfg = getHDFSParquetImporterConfig(srcFolder.toString(), hoodieFolder.toString(),
        "testTable", "COPY_ON_WRITE", "_row_key", "timestamp", 1, schemaFile);
    AtomicInteger retry = new AtomicInteger(3);
    AtomicInteger fileCreated = new AtomicInteger(0);
    HDFSParquetImporter dataImporter = new HDFSParquetImporter(cfg) {
      @Override
      protected int dataImport(JavaSparkContext jsc) throws IOException {
        int ret = super.dataImport(jsc);
        if (retry.decrementAndGet() == 0) {
          fileCreated.incrementAndGet();
          createSchemaFile(schemaFile);
        }

        return ret;
      }
    };
    // Schema file is not created so this operation should fail.
    assertEquals(0, dataImporter.dataImport(jsc(), retry.get()));
    assertEquals(-1, retry.get());
    assertEquals(1, fileCreated.get());

    // Check if
    // 1. .commit file is present
    // 2. number of records in each partition == 24
    // 3. total number of partitions == 4;
    boolean isCommitFilePresent = false;
    Map<String, Long> recordCounts = new HashMap<String, Long>();
    RemoteIterator<LocatedFileStatus> hoodieFiles = dfs().listFiles(hoodieFolder, true);
    while (hoodieFiles.hasNext()) {
      LocatedFileStatus f = hoodieFiles.next();
      isCommitFilePresent = isCommitFilePresent || f.getPath().toString().endsWith(HoodieTimeline.COMMIT_EXTENSION);

      if (f.getPath().toString().endsWith("parquet")) {
        String partitionPath = f.getPath().getParent().toString();
        long count = sqlContext().read().parquet(f.getPath().toString()).count();
        if (!recordCounts.containsKey(partitionPath)) {
          recordCounts.put(partitionPath, 0L);
        }
        recordCounts.put(partitionPath, recordCounts.get(partitionPath) + count);
      }
    }
    assertTrue(isCommitFilePresent, "commit file is missing");
    assertEquals(4, recordCounts.size(), "partition is missing");
    for (Entry<String, Long> e : recordCounts.entrySet()) {
      assertEquals(24, e.getValue().longValue(), "missing records");
    }
  }

  private void insert(JavaSparkContext jsc) throws IOException {
    // Create schema file.
    String schemaFile = new Path(basePath, "file.schema").toString();
    createSchemaFile(schemaFile);

    HDFSParquetImporter.Config cfg = getHDFSParquetImporterConfig(srcFolder.toString(), hoodieFolder.toString(),
        "testTable", "COPY_ON_WRITE", "_row_key", "timestamp", 1, schemaFile);
    HDFSParquetImporter dataImporter = new HDFSParquetImporter(cfg);

    dataImporter.dataImport(jsc, 0);
  }

  /**
   * Test successful insert and verify data consistency.
   */
  @Test
  public void testImportWithInsert() throws IOException, ParseException {
    insert(jsc());
    Dataset<Row> ds = HoodieClientTestUtils.read(jsc(), basePath + "/testTarget", sqlContext(), dfs(), basePath + "/testTarget/*/*/*/*");

    List<Row> readData = ds.select("timestamp", "_row_key", "rider", "driver", "begin_lat", "begin_lon", "end_lat", "end_lon").collectAsList();
    List<HoodieTripModel> result = readData.stream().map(row ->
        new HoodieTripModel(row.getLong(0), row.getString(1), row.getString(2), row.getString(3), row.getDouble(4),
            row.getDouble(5), row.getDouble(6), row.getDouble(7)))
        .collect(Collectors.toList());

    List<HoodieTripModel> expected = insertData.stream().map(g ->
        new HoodieTripModel(Long.parseLong(g.get("timestamp").toString()),
            g.get("_row_key").toString(),
            g.get("rider").toString(),
            g.get("driver").toString(),
            Double.parseDouble(g.get("begin_lat").toString()),
            Double.parseDouble(g.get("begin_lon").toString()),
            Double.parseDouble(g.get("end_lat").toString()),
            Double.parseDouble(g.get("end_lon").toString())))
        .collect(Collectors.toList());

    assertTrue(result.containsAll(expected) && expected.containsAll(result) && result.size() == expected.size());
  }

  /**
   * Test upsert data and verify data consistency.
   */
  @Test
  public void testImportWithUpsert() throws IOException, ParseException {
    insert(jsc());

    // Create schema file.
    String schemaFile = new Path(basePath, "file.schema").toString();

    Path upsertFolder = new Path(basePath, "testUpsertSrc");
    List<GenericRecord> upsertData = createUpsertRecords(upsertFolder);

    HDFSParquetImporter.Config cfg = getHDFSParquetImporterConfig(upsertFolder.toString(), hoodieFolder.toString(),
        "testTable", "COPY_ON_WRITE", "_row_key", "timestamp", 1, schemaFile);
    cfg.command = "upsert";
    HDFSParquetImporter dataImporter = new HDFSParquetImporter(cfg);

    dataImporter.dataImport(jsc(), 0);

    // construct result, remove top 10 and add upsert data.
    List<GenericRecord> expectData = insertData.subList(11, 96);
    expectData.addAll(upsertData);

    // read latest data
    Dataset<Row> ds = HoodieClientTestUtils.read(jsc(), basePath + "/testTarget", sqlContext(), dfs(), basePath + "/testTarget/*/*/*/*");

    List<Row> readData = ds.select("timestamp", "_row_key", "rider", "driver", "begin_lat", "begin_lon", "end_lat", "end_lon").collectAsList();
    List<HoodieTripModel> result = readData.stream().map(row ->
        new HoodieTripModel(row.getLong(0), row.getString(1), row.getString(2), row.getString(3), row.getDouble(4),
            row.getDouble(5), row.getDouble(6), row.getDouble(7)))
        .collect(Collectors.toList());

    // get expected result.
    List<HoodieTripModel> expected = expectData.stream().map(g ->
        new HoodieTripModel(Long.parseLong(g.get("timestamp").toString()),
            g.get("_row_key").toString(),
            g.get("rider").toString(),
            g.get("driver").toString(),
            Double.parseDouble(g.get("begin_lat").toString()),
            Double.parseDouble(g.get("begin_lon").toString()),
            Double.parseDouble(g.get("end_lat").toString()),
            Double.parseDouble(g.get("end_lon").toString())))
        .collect(Collectors.toList());

    assertTrue(result.containsAll(expected) && expected.containsAll(result) && result.size() == expected.size());
  }

  public List<GenericRecord> createInsertRecords(Path srcFolder) throws ParseException, IOException {
    Path srcFile = new Path(srcFolder.toString(), "file1.parquet");
    long startTime = HoodieActiveTimeline.parseInstantTime("20170203000000").getTime() / 1000;
    List<GenericRecord> records = new ArrayList<GenericRecord>();
    for (long recordNum = 0; recordNum < 96; recordNum++) {
      records.add(HoodieTestDataGenerator.generateGenericRecord(Long.toString(recordNum), "rider-" + recordNum,
          "driver-" + recordNum, startTime + TimeUnit.HOURS.toSeconds(recordNum)));
    }
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(srcFile)
        .withSchema(HoodieTestDataGenerator.AVRO_SCHEMA).withConf(HoodieTestUtils.getDefaultHadoopConf()).build()) {
      for (GenericRecord record : records) {
        writer.write(record);
      }
    }
    return records;
  }

  public List<GenericRecord> createUpsertRecords(Path srcFolder) throws ParseException, IOException {
    Path srcFile = new Path(srcFolder.toString(), "file1.parquet");
    long startTime = HoodieActiveTimeline.parseInstantTime("20170203000000").getTime() / 1000;
    List<GenericRecord> records = new ArrayList<GenericRecord>();
    // 10 for update
    for (long recordNum = 0; recordNum < 11; recordNum++) {
      records.add(HoodieTestDataGenerator.generateGenericRecord(Long.toString(recordNum), "rider-upsert-" + recordNum,
          "driver-upsert" + recordNum, startTime + TimeUnit.HOURS.toSeconds(recordNum)));
    }
    // 4 for insert
    for (long recordNum = 96; recordNum < 100; recordNum++) {
      records.add(HoodieTestDataGenerator.generateGenericRecord(Long.toString(recordNum), "rider-upsert-" + recordNum,
          "driver-upsert" + recordNum, startTime + TimeUnit.HOURS.toSeconds(recordNum)));
    }
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(srcFile)
        .withSchema(HoodieTestDataGenerator.AVRO_SCHEMA).withConf(HoodieTestUtils.getDefaultHadoopConf()).build()) {
      for (GenericRecord record : records) {
        writer.write(record);
      }
    }
    return records;
  }

  private void createSchemaFile(String schemaFile) throws IOException {
    FSDataOutputStream schemaFileOS = dfs().create(new Path(schemaFile));
    schemaFileOS.write(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA.getBytes());
    schemaFileOS.close();
  }

  /**
   * Tests for scheme file. 1. File is missing. 2. File has invalid data.
   */
  @Test
  public void testSchemaFile() throws Exception {
    // Hoodie root folder
    Path hoodieFolder = new Path(basePath, "testTarget");
    Path srcFolder = new Path(basePath.toString(), "srcTest");
    Path schemaFile = new Path(basePath.toString(), "missingFile.schema");
    HDFSParquetImporter.Config cfg = getHDFSParquetImporterConfig(srcFolder.toString(), hoodieFolder.toString(),
        "testTable", "COPY_ON_WRITE", "_row_key", "timestamp", 1, schemaFile.toString());
    HDFSParquetImporter dataImporter = new HDFSParquetImporter(cfg);
    // Should fail - return : -1.
    assertEquals(-1, dataImporter.dataImport(jsc(), 0));

    dfs().create(schemaFile).write("Random invalid schema data".getBytes());
    // Should fail - return : -1.
    assertEquals(-1, dataImporter.dataImport(jsc(), 0));
  }

  /**
   * Test for missing rowKey and partitionKey.
   */
  @Test
  public void testRowAndPartitionKey() throws Exception {
    // Create schema file.
    Path schemaFile = new Path(basePath.toString(), "missingFile.schema");
    createSchemaFile(schemaFile.toString());

    HDFSParquetImporter dataImporter;
    HDFSParquetImporter.Config cfg;

    // Check for invalid row key.
    cfg = getHDFSParquetImporterConfig(srcFolder.toString(), hoodieFolder.toString(), "testTable", "COPY_ON_WRITE",
        "invalidRowKey", "timestamp", 1, schemaFile.toString());
    dataImporter = new HDFSParquetImporter(cfg);
    assertEquals(-1, dataImporter.dataImport(jsc(), 0));

    // Check for invalid partition key.
    cfg = getHDFSParquetImporterConfig(srcFolder.toString(), hoodieFolder.toString(), "testTable", "COPY_ON_WRITE",
        "_row_key", "invalidTimeStamp", 1, schemaFile.toString());
    dataImporter = new HDFSParquetImporter(cfg);
    assertEquals(-1, dataImporter.dataImport(jsc(), 0));
  }

  public HDFSParquetImporter.Config getHDFSParquetImporterConfig(String srcPath, String targetPath, String tableName,
      String tableType, String rowKey, String partitionKey, int parallelism, String schemaFile) {
    HDFSParquetImporter.Config cfg = new HDFSParquetImporter.Config();
    cfg.srcPath = srcPath;
    cfg.targetPath = targetPath;
    cfg.tableName = tableName;
    cfg.tableType = tableType;
    cfg.rowKey = rowKey;
    cfg.partitionKey = partitionKey;
    cfg.parallelism = parallelism;
    cfg.schemaFile = schemaFile;
    return cfg;
  }

  /**
   * Class used for compare result and expected.
   */
  public static class HoodieTripModel {

    long timestamp;
    String rowKey;
    String rider;
    String driver;
    double beginLat;
    double beginLon;
    double endLat;
    double endLon;

    public HoodieTripModel(long timestamp, String rowKey, String rider, String driver, double beginLat,
        double beginLon, double endLat, double endLon) {
      this.timestamp = timestamp;
      this.rowKey = rowKey;
      this.rider = rider;
      this.driver = driver;
      this.beginLat = beginLat;
      this.beginLon = beginLon;
      this.endLat = endLat;
      this.endLon = endLon;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      HoodieTripModel other = (HoodieTripModel) o;
      return timestamp == other.timestamp && rowKey.equals(other.rowKey) && rider.equals(other.rider)
          && driver.equals(other.driver) && beginLat == other.beginLat && beginLon == other.beginLon
          && endLat == other.endLat && endLon == other.endLon;
    }

    @Override
    public int hashCode() {
      return Objects.hash(timestamp, rowKey, rider, driver, beginLat, beginLon, endLat, endLon);
    }
  }
}

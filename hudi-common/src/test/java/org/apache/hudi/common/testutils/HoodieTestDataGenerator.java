/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.testutils;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Class to be used in tests to keep generating test inserts and updates against a corpus.
 * <p>
 * Test data uses a toy Uber trips, data model.
 */
public class HoodieTestDataGenerator {

  // based on examination of sample file, the schema produces the following per record size
  public static final int BYTES_PER_RECORD = (int) (1.2 * 1024);
  // with default bloom filter with 60,000 entries and 0.000000001 FPRate
  public static final int BLOOM_FILTER_BYTES = 323495;
  private static Logger logger = LogManager.getLogger(HoodieTestDataGenerator.class);
  public static final String DEFAULT_FIRST_PARTITION_PATH = "2016/03/15";
  public static final String DEFAULT_SECOND_PARTITION_PATH = "2015/03/16";
  public static final String DEFAULT_THIRD_PARTITION_PATH = "2015/03/17";

  public static final String[] DEFAULT_PARTITION_PATHS =
      {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH};
  public static final int DEFAULT_PARTITION_DEPTH = 3;
  public static final String TRIP_SCHEMA_PREFIX = "{\"type\": \"record\"," + "\"name\": \"triprec\"," + "\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"long\"}," + "{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"rider\", \"type\": \"string\"}," + "{\"name\": \"driver\", \"type\": \"string\"},"
      + "{\"name\": \"begin_lat\", \"type\": \"double\"}," + "{\"name\": \"begin_lon\", \"type\": \"double\"},"
      + "{\"name\": \"end_lat\", \"type\": \"double\"}," + "{\"name\": \"end_lon\", \"type\": \"double\"},";
  public static final String TRIP_SCHEMA_SUFFIX = "{\"name\": \"_hoodie_is_deleted\", \"type\": \"boolean\", \"default\": false} ]}";
  public static final String FARE_NESTED_SCHEMA = "{\"name\": \"fare\",\"type\": {\"type\":\"record\", \"name\":\"fare\",\"fields\": ["
      + "{\"name\": \"amount\",\"type\": \"double\"},{\"name\": \"currency\", \"type\": \"string\"}]}},";
  public static final String FARE_FLATTENED_SCHEMA = "{\"name\": \"fare\", \"type\": \"double\"},"
      + "{\"name\": \"currency\", \"type\": \"string\"},";
  public static final String TIP_NESTED_SCHEMA = "{\"name\": \"tip_history\", \"default\": [], \"type\": {\"type\": "
      + "\"array\", \"default\": [], \"items\": {\"type\": \"record\", \"default\": null, \"name\": \"tip_history\", \"fields\": ["
      + "{\"name\": \"amount\", \"type\": \"double\"}, {\"name\": \"currency\", \"type\": \"string\"}]}}},";
  public static final String MAP_TYPE_SCHEMA = "{\"name\": \"city_to_state\", \"type\": {\"type\": \"map\", \"values\": \"string\"}},";
  public static final String EXTRA_TYPE_SCHEMA = "{\"name\": \"distance_in_meters\", \"type\": \"int\"},"
      + "{\"name\": \"seconds_since_epoch\", \"type\": \"long\"},"
      + "{\"name\": \"weight\", \"type\": \"float\"},"
      + "{\"name\": \"nation\", \"type\": \"bytes\"},"
      + "{\"name\":\"current_date\",\"type\": {\"type\": \"int\", \"logicalType\": \"date\"}},"
      + "{\"name\":\"current_ts\",\"type\": {\"type\": \"long\"}},"
      + "{\"name\":\"height\",\"type\":{\"type\":\"fixed\",\"name\":\"fixed\",\"size\":5,\"logicalType\":\"decimal\",\"precision\":10,\"scale\":6}},";

  public static final String TRIP_EXAMPLE_SCHEMA =
      TRIP_SCHEMA_PREFIX + EXTRA_TYPE_SCHEMA + MAP_TYPE_SCHEMA + FARE_NESTED_SCHEMA + TIP_NESTED_SCHEMA + TRIP_SCHEMA_SUFFIX;
  public static final String TRIP_FLATTENED_SCHEMA =
      TRIP_SCHEMA_PREFIX + FARE_FLATTENED_SCHEMA + TRIP_SCHEMA_SUFFIX;

  public static final String TRIP_SCHEMA = "{\"type\":\"record\",\"name\":\"tripUberRec\",\"fields\":["
      + "{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"_row_key\",\"type\":\"string\"},{\"name\":\"rider\",\"type\":\"string\"},"
      + "{\"name\":\"driver\",\"type\":\"string\"},{\"name\":\"fare\",\"type\":\"double\"},{\"name\": \"_hoodie_is_deleted\", \"type\": \"boolean\", \"default\": false}]}";
  public static final String SHORT_TRIP_SCHEMA = "{\"type\":\"record\",\"name\":\"shortTripRec\",\"fields\":["
      + "{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"_row_key\",\"type\":\"string\"},{\"name\":\"rider\",\"type\":\"string\"},"
      + "{\"name\":\"driver\",\"type\":\"string\"},{\"name\":\"fare\",\"type\":\"double\"},{\"name\": \"_hoodie_is_deleted\", \"type\": \"boolean\", \"default\": false}]}";

  public static final String NULL_SCHEMA = Schema.create(Schema.Type.NULL).toString();
  public static final String TRIP_HIVE_COLUMN_TYPES = "bigint,string,string,string,double,double,double,double,int,bigint,float,binary,int,bigint,decimal(10,6),"
      + "map<string,string>,struct<amount:double,currency:string>,array<struct<amount:double,currency:string>>,boolean";


  public static final Schema AVRO_SCHEMA = new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA);
  public static final Schema AVRO_SCHEMA_WITH_METADATA_FIELDS =
      HoodieAvroUtils.addMetadataFields(AVRO_SCHEMA);
  public static final Schema AVRO_SHORT_TRIP_SCHEMA = new Schema.Parser().parse(SHORT_TRIP_SCHEMA);
  public static final Schema AVRO_TRIP_SCHEMA = new Schema.Parser().parse(TRIP_SCHEMA);
  public static final Schema FLATTENED_AVRO_SCHEMA = new Schema.Parser().parse(TRIP_FLATTENED_SCHEMA);

  private static final Random RAND = new Random(46474747);

  //Maintains all the existing keys schema wise
  private final Map<String, Map<Integer, KeyPartition>> existingKeysBySchema;
  private final String[] partitionPaths;
  //maintains the count of existing keys schema wise
  private Map<String, Integer> numKeysBySchema;

  public HoodieTestDataGenerator(String[] partitionPaths) {
    this(partitionPaths, new HashMap<>());
  }

  public HoodieTestDataGenerator() {
    this(DEFAULT_PARTITION_PATHS);
  }

  public HoodieTestDataGenerator(String[] partitionPaths, Map<Integer, KeyPartition> keyPartitionMap) {
    this.partitionPaths = Arrays.copyOf(partitionPaths, partitionPaths.length);
    this.existingKeysBySchema = new HashMap<>();
    existingKeysBySchema.put(TRIP_EXAMPLE_SCHEMA, keyPartitionMap);
    numKeysBySchema = new HashMap<>();
  }

  /**
   * @implNote {@link HoodieTestDataGenerator} is supposed to just generate records with schemas. Leave HoodieTable files (metafile, basefile, logfile, etc) to {@link HoodieTestTable}.
   * @deprecated Use {@link HoodieTestTable#withPartitionMetaFiles(java.lang.String...)} instead.
   */
  public static void writePartitionMetadata(FileSystem fs, String[] partitionPaths, String basePath) {
    for (String partitionPath : partitionPaths) {
      new HoodiePartitionMetadata(fs, "000", new Path(basePath), new Path(basePath, partitionPath)).trySave(0);
    }
  }

  public int getEstimatedFileSizeInBytes(int numOfRecords) {
    return numOfRecords * BYTES_PER_RECORD + BLOOM_FILTER_BYTES;
  }

  public RawTripTestPayload generateRandomValueAsPerSchema(String schemaStr, HoodieKey key, String commitTime, boolean isFlattened) throws IOException {
    if (TRIP_EXAMPLE_SCHEMA.equals(schemaStr)) {
      return generateRandomValue(key, commitTime, isFlattened);
    } else if (TRIP_SCHEMA.equals(schemaStr)) {
      return generatePayloadForTripSchema(key, commitTime);
    } else if (SHORT_TRIP_SCHEMA.equals(schemaStr)) {
      return generatePayloadForShortTripSchema(key, commitTime);
    }

    return null;
  }

  /**
   * Generates a new avro record of the above nested schema format,
   * retaining the key if optionally provided.
   *
   * @param key Hoodie key.
   * @param instantTime Instant time to use.
   * @return Raw paylaod of a test record.
   */
  public static RawTripTestPayload generateRandomValue(HoodieKey key, String instantTime) throws IOException {
    return generateRandomValue(key, instantTime, false);
  }

  /**
   * Generates a new avro record with the specified schema (nested or flattened),
   * retaining the key if optionally provided.
   *
   * @param key  Hoodie key.
   * @param instantTime  Commit time to use.
   * @param isFlattened  whether the schema of the record should be flattened.
   * @return Raw paylaod of a test record.
   * @throws IOException
   */
  public static RawTripTestPayload generateRandomValue(
      HoodieKey key, String instantTime, boolean isFlattened) throws IOException {
    GenericRecord rec = generateGenericRecord(
        key.getRecordKey(), "rider-" + instantTime, "driver-" + instantTime, 0,
        false, isFlattened);
    return new RawTripTestPayload(rec.toString(), key.getRecordKey(), key.getPartitionPath(), TRIP_EXAMPLE_SCHEMA);
  }

  /**
   * Generates a new avro record with TRIP_SCHEMA, retaining the key if optionally provided.
   */
  public RawTripTestPayload generatePayloadForTripSchema(HoodieKey key, String commitTime) throws IOException {
    GenericRecord rec = generateRecordForTripSchema(key.getRecordKey(), "rider-" + commitTime, "driver-" + commitTime, 0);
    return new RawTripTestPayload(rec.toString(), key.getRecordKey(), key.getPartitionPath(), TRIP_SCHEMA);
  }

  public RawTripTestPayload generatePayloadForShortTripSchema(HoodieKey key, String commitTime) throws IOException {
    GenericRecord rec = generateRecordForShortTripSchema(key.getRecordKey(), "rider-" + commitTime, "driver-" + commitTime, 0);
    return new RawTripTestPayload(rec.toString(), key.getRecordKey(), key.getPartitionPath(), SHORT_TRIP_SCHEMA);
  }

  /**
   * Generates a new avro record of the above schema format for a delete.
   */
  public static RawTripTestPayload generateRandomDeleteValue(HoodieKey key, String instantTime) throws IOException {
    GenericRecord rec = generateGenericRecord(key.getRecordKey(), "rider-" + instantTime, "driver-" + instantTime, 0,
        true, false);
    return new RawTripTestPayload(Option.of(rec.toString()), key.getRecordKey(), key.getPartitionPath(), TRIP_EXAMPLE_SCHEMA, true);
  }

  /**
   * Generates a new avro record of the above schema format, retaining the key if optionally provided.
   */
  public static HoodieAvroPayload generateAvroPayload(HoodieKey key, String instantTime) {
    GenericRecord rec = generateGenericRecord(key.getRecordKey(), "rider-" + instantTime, "driver-" + instantTime, 0);
    return new HoodieAvroPayload(Option.of(rec));
  }

  public static GenericRecord generateGenericRecord(String rowKey, String riderName, String driverName,
                                                    long timestamp) {
    return generateGenericRecord(rowKey, riderName, driverName, timestamp, false, false);
  }

  public static GenericRecord generateGenericRecord(String rowKey, String riderName, String driverName,
                                                    long timestamp, boolean isDeleteRecord,
                                                    boolean isFlattened) {
    GenericRecord rec = new GenericData.Record(isFlattened ? FLATTENED_AVRO_SCHEMA : AVRO_SCHEMA);
    rec.put("_row_key", rowKey);
    rec.put("timestamp", timestamp);
    rec.put("rider", riderName);
    rec.put("driver", driverName);
    rec.put("begin_lat", RAND.nextDouble());
    rec.put("begin_lon", RAND.nextDouble());
    rec.put("end_lat", RAND.nextDouble());
    rec.put("end_lon", RAND.nextDouble());
    if (isFlattened) {
      rec.put("fare", RAND.nextDouble() * 100);
      rec.put("currency", "USD");
    } else {
      rec.put("distance_in_meters", RAND.nextInt());
      rec.put("seconds_since_epoch", RAND.nextLong());
      rec.put("weight", RAND.nextFloat());
      byte[] bytes = "Canada".getBytes();
      rec.put("nation", ByteBuffer.wrap(bytes));
      long currentTimeMillis = System.currentTimeMillis();
      Date date = new Date(currentTimeMillis);
      rec.put("current_date", (int) date.toLocalDate().toEpochDay());
      rec.put("current_ts", currentTimeMillis);

      BigDecimal bigDecimal = new BigDecimal(String.format("%5f", RAND.nextFloat()));
      Schema decimalSchema = AVRO_SCHEMA.getField("height").schema();
      Conversions.DecimalConversion decimalConversions = new Conversions.DecimalConversion();
      GenericFixed genericFixed = decimalConversions.toFixed(bigDecimal, decimalSchema, LogicalTypes.decimal(10, 6));
      rec.put("height", genericFixed);

      rec.put("city_to_state", Collections.singletonMap("LA", "CA"));

      GenericRecord fareRecord = new GenericData.Record(AVRO_SCHEMA.getField("fare").schema());
      fareRecord.put("amount", RAND.nextDouble() * 100);
      fareRecord.put("currency", "USD");
      rec.put("fare", fareRecord);

      GenericArray<GenericRecord> tipHistoryArray = new GenericData.Array<>(1, AVRO_SCHEMA.getField("tip_history").schema());
      Schema tipSchema = new Schema.Parser().parse(AVRO_SCHEMA.getField("tip_history").schema().toString()).getElementType();
      GenericRecord tipRecord = new GenericData.Record(tipSchema);
      tipRecord.put("amount", RAND.nextDouble() * 100);
      tipRecord.put("currency", "USD");
      tipHistoryArray.add(tipRecord);
      rec.put("tip_history", tipHistoryArray);
    }

    if (isDeleteRecord) {
      rec.put("_hoodie_is_deleted", true);
    } else {
      rec.put("_hoodie_is_deleted", false);
    }
    return rec;
  }

  /*
  Generate random record using TRIP_SCHEMA
   */
  public GenericRecord generateRecordForTripSchema(String rowKey, String riderName, String driverName, long timestamp) {
    GenericRecord rec = new GenericData.Record(AVRO_TRIP_SCHEMA);
    rec.put("_row_key", rowKey);
    rec.put("timestamp", timestamp);
    rec.put("rider", riderName);
    rec.put("driver", driverName);
    rec.put("fare", RAND.nextDouble() * 100);
    rec.put("_hoodie_is_deleted", false);
    return rec;
  }

  public GenericRecord generateRecordForShortTripSchema(String rowKey, String riderName, String driverName, long timestamp) {
    GenericRecord rec = new GenericData.Record(AVRO_SHORT_TRIP_SCHEMA);
    rec.put("_row_key", rowKey);
    rec.put("timestamp", timestamp);
    rec.put("rider", riderName);
    rec.put("driver", driverName);
    rec.put("fare", RAND.nextDouble() * 100);
    rec.put("_hoodie_is_deleted", false);
    return rec;
  }

  public static void createCommitFile(String basePath, String instantTime, Configuration configuration) {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    createCommitFile(basePath, instantTime, configuration, commitMetadata);
  }

  public static void createCommitFile(String basePath, String instantTime, Configuration configuration, HoodieCommitMetadata commitMetadata) {
    Arrays.asList(HoodieTimeline.makeCommitFileName(instantTime), HoodieTimeline.makeInflightCommitFileName(instantTime),
        HoodieTimeline.makeRequestedCommitFileName(instantTime))
        .forEach(f -> createMetadataFile(f, basePath, configuration, commitMetadata));
  }

  private static void createMetadataFile(String f, String basePath, Configuration configuration, HoodieCommitMetadata commitMetadata) {
    try {
      createMetadataFile(f, basePath, configuration, commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  private static void createMetadataFile(String f, String basePath, Configuration configuration, byte[] content) {
    Path commitFile = new Path(
        basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + f);
    FSDataOutputStream os = null;
    try {
      FileSystem fs = FSUtils.getFs(basePath, configuration);
      os = fs.create(commitFile, true);
      // Write empty commit metadata
      os.write(content);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    } finally {
      if (null != os) {
        try {
          os.close();
        } catch (IOException e) {
          throw new HoodieIOException(e.getMessage(), e);
        }
      }
    }
  }

  public static void createReplaceFile(String basePath, String instantTime, Configuration configuration, HoodieCommitMetadata commitMetadata) {
    Arrays.asList(HoodieTimeline.makeReplaceFileName(instantTime), HoodieTimeline.makeInflightReplaceFileName(instantTime),
        HoodieTimeline.makeRequestedReplaceFileName(instantTime))
        .forEach(f -> createMetadataFile(f, basePath, configuration, commitMetadata));
  }

  public static void createEmptyCleanRequestedFile(String basePath, String instantTime, Configuration configuration)
      throws IOException {
    Path commitFile = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + HoodieTimeline.makeRequestedCleanerFileName(instantTime));
    createEmptyFile(basePath, commitFile, configuration);
  }

  public static void createCompactionRequestedFile(String basePath, String instantTime, Configuration configuration)
      throws IOException {
    Path commitFile = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + HoodieTimeline.makeRequestedCompactionFileName(instantTime));
    createEmptyFile(basePath, commitFile, configuration);
  }

  private static void createEmptyFile(String basePath, Path filePath, Configuration configuration) throws IOException {
    FileSystem fs = FSUtils.getFs(basePath, configuration);
    FSDataOutputStream os = fs.create(filePath, true);
    os.close();
  }

  public static void createCompactionAuxiliaryMetadata(String basePath, HoodieInstant instant,
                                                       Configuration configuration) throws IOException {
    Path commitFile =
        new Path(basePath + "/" + HoodieTableMetaClient.AUXILIARYFOLDER_NAME + "/" + instant.getFileName());
    FileSystem fs = FSUtils.getFs(basePath, configuration);
    try (FSDataOutputStream os = fs.create(commitFile, true)) {
      HoodieCompactionPlan workload = HoodieCompactionPlan.newBuilder().setVersion(1).build();
      // Write empty commit metadata
      os.write(TimelineMetadataUtils.serializeCompactionPlan(workload).get());
    }
  }

  public static void createSavepointFile(String basePath, String instantTime, Configuration configuration)
      throws IOException {
    Path commitFile = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + HoodieTimeline.makeSavePointFileName(instantTime));
    FileSystem fs = FSUtils.getFs(basePath, configuration);
    try (FSDataOutputStream os = fs.create(commitFile, true)) {
      HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
      // Write empty commit metadata
      os.writeBytes(new String(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    }
  }

  public List<HoodieRecord> generateInsertsAsPerSchema(String commitTime, Integer n, String schemaStr) {
    return generateInsertsStream(commitTime, n, false, schemaStr).collect(Collectors.toList());
  }

  /**
   * Generates new inserts with nested schema, uniformly across the partition paths above.
   * It also updates the list of existing keys.
   */
  public List<HoodieRecord> generateInserts(String instantTime, Integer n) {
    return generateInserts(instantTime, n, false);
  }

  /**
   * Generates new inserts, uniformly across the partition paths above.
   * It also updates the list of existing keys.
   *
   * @param instantTime  Commit time to use.
   * @param n  Number of records.
   * @param isFlattened  whether the schema of the generated record is flattened
   * @return  List of {@link HoodieRecord}s
   */
  public List<HoodieRecord> generateInserts(String instantTime, Integer n, boolean isFlattened) {
    return generateInsertsStream(instantTime, n, isFlattened, TRIP_EXAMPLE_SCHEMA).collect(Collectors.toList());
  }

  /**
   * Generates new inserts, uniformly across the partition paths above. It also updates the list of existing keys.
   */
  public Stream<HoodieRecord> generateInsertsStream(String commitTime, Integer n, boolean isFlattened, String schemaStr) {
    return generateInsertsStream(commitTime, n, isFlattened, schemaStr, false);
  }

  public List<HoodieRecord> generateInsertsContainsAllPartitions(String instantTime, Integer n) {
    if (n < partitionPaths.length) {
      throw new HoodieIOException("n must greater then partitionPaths length");
    }
    return generateInsertsStream(instantTime,  n, false, TRIP_EXAMPLE_SCHEMA, true).collect(Collectors.toList());
  }

  public List<HoodieRecord> generateInsertsForPartition(String instantTime, Integer n, String partition) {
    return generateInsertsStream(instantTime,  n, false, TRIP_EXAMPLE_SCHEMA, false, () -> partition, () -> UUID.randomUUID().toString()).collect(Collectors.toList());
  }

  public Stream<HoodieRecord> generateInsertsStream(String commitTime, Integer n, boolean isFlattened, String schemaStr, boolean containsAllPartitions) {
    return generateInsertsStream(commitTime, n, isFlattened, schemaStr, containsAllPartitions,
        () -> partitionPaths[RAND.nextInt(partitionPaths.length)],
        () -> UUID.randomUUID().toString());
  }

  /**
   * Generates new inserts, uniformly across the partition paths above. It also updates the list of existing keys.
   */
  public Stream<HoodieRecord> generateInsertsStream(String instantTime, Integer n, boolean isFlattened, String schemaStr, boolean containsAllPartitions,
                                                    Supplier<String> partitionPathSupplier, Supplier<String> recordKeySupplier) {
    int currSize = getNumExistingKeys(schemaStr);
    return IntStream.range(0, n).boxed().map(i -> {
      String partitionPath = partitionPathSupplier.get();
      if (containsAllPartitions && i < partitionPaths.length) {
        partitionPath = partitionPaths[i];
      }
      HoodieKey key = new HoodieKey(recordKeySupplier.get(), partitionPath);
      KeyPartition kp = new KeyPartition();
      kp.key = key;
      kp.partitionPath = partitionPath;
      populateKeysBySchema(schemaStr, currSize + i, kp);
      incrementNumExistingKeysBySchema(schemaStr);
      try {
        return new HoodieRecord(key, generateRandomValueAsPerSchema(schemaStr, key, instantTime, isFlattened));
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    });
  }

  /*
  Takes care of populating keys schema wise
   */
  private void populateKeysBySchema(String schemaStr, int i, KeyPartition kp) {
    if (existingKeysBySchema.containsKey(schemaStr)) {
      existingKeysBySchema.get(schemaStr).put(i, kp);
    } else {
      existingKeysBySchema.put(schemaStr, new HashMap<>());
      existingKeysBySchema.get(schemaStr).put(i, kp);
    }
  }

  private void incrementNumExistingKeysBySchema(String schemaStr) {
    if (numKeysBySchema.containsKey(schemaStr)) {
      numKeysBySchema.put(schemaStr, numKeysBySchema.get(schemaStr) + 1);
    } else {
      numKeysBySchema.put(schemaStr, 1);
    }
  }

  public List<HoodieRecord> generateSameKeyInserts(String instantTime, List<HoodieRecord> origin) throws IOException {
    List<HoodieRecord> copy = new ArrayList<>();
    for (HoodieRecord r : origin) {
      HoodieKey key = r.getKey();
      HoodieRecord record = new HoodieRecord(key, generateRandomValue(key, instantTime));
      copy.add(record);
    }
    return copy;
  }

  public List<HoodieRecord> generateInsertsWithHoodieAvroPayload(String instantTime, int limit) {
    List<HoodieRecord> inserts = new ArrayList<>();
    int currSize = getNumExistingKeys(TRIP_EXAMPLE_SCHEMA);
    for (int i = 0; i < limit; i++) {
      String partitionPath = partitionPaths[RAND.nextInt(partitionPaths.length)];
      HoodieKey key = new HoodieKey(UUID.randomUUID().toString(), partitionPath);
      HoodieRecord record = new HoodieRecord(key, generateAvroPayload(key, instantTime));
      inserts.add(record);

      KeyPartition kp = new KeyPartition();
      kp.key = key;
      kp.partitionPath = partitionPath;
      populateKeysBySchema(TRIP_EXAMPLE_SCHEMA, currSize + i, kp);
      incrementNumExistingKeysBySchema(TRIP_EXAMPLE_SCHEMA);
    }
    return inserts;
  }

  public List<HoodieRecord> generateUpdatesWithHoodieAvroPayload(String instantTime, List<HoodieRecord> baseRecords) {
    List<HoodieRecord> updates = new ArrayList<>();
    for (HoodieRecord baseRecord : baseRecords) {
      HoodieRecord record = new HoodieRecord(baseRecord.getKey(), generateAvroPayload(baseRecord.getKey(), instantTime));
      updates.add(record);
    }
    return updates;
  }

  public List<HoodieRecord> generateDeletes(String instantTime, Integer n) throws IOException {
    List<HoodieRecord> inserts = generateInserts(instantTime, n);
    return generateDeletesFromExistingRecords(inserts);
  }

  public List<HoodieRecord> generateDeletesFromExistingRecords(List<HoodieRecord> existingRecords) throws IOException {
    List<HoodieRecord> deletes = new ArrayList<>();
    for (HoodieRecord existingRecord : existingRecords) {
      HoodieRecord record = generateDeleteRecord(existingRecord);
      deletes.add(record);
    }
    return deletes;
  }

  public HoodieRecord generateDeleteRecord(HoodieRecord existingRecord) throws IOException {
    HoodieKey key = existingRecord.getKey();
    return generateDeleteRecord(key);
  }

  public HoodieRecord generateDeleteRecord(HoodieKey key) throws IOException {
    RawTripTestPayload payload =
        new RawTripTestPayload(Option.empty(), key.getRecordKey(), key.getPartitionPath(), null, true);
    return new HoodieRecord(key, payload);
  }

  public HoodieRecord generateUpdateRecord(HoodieKey key, String instantTime) throws IOException {
    return new HoodieRecord(key, generateRandomValue(key, instantTime));
  }

  public List<HoodieRecord> generateUpdates(String instantTime, List<HoodieRecord> baseRecords) throws IOException {
    List<HoodieRecord> updates = new ArrayList<>();
    for (HoodieRecord baseRecord : baseRecords) {
      HoodieRecord record = generateUpdateRecord(baseRecord.getKey(), instantTime);
      updates.add(record);
    }
    return updates;
  }

  public List<HoodieRecord> generateUpdatesWithDiffPartition(String instantTime, List<HoodieRecord> baseRecords)
      throws IOException {
    List<HoodieRecord> updates = new ArrayList<>();
    for (HoodieRecord baseRecord : baseRecords) {
      String partition = baseRecord.getPartitionPath();
      String newPartition = "";
      if (partitionPaths[0].equalsIgnoreCase(partition)) {
        newPartition = partitionPaths[1];
      } else {
        newPartition = partitionPaths[0];
      }
      HoodieKey key = new HoodieKey(baseRecord.getRecordKey(), newPartition);
      HoodieRecord record = generateUpdateRecord(key, instantTime);
      updates.add(record);
    }
    return updates;
  }

  /**
   * Generates new updates, randomly distributed across the keys above. There can be duplicates within the returned
   * list
   *
   * @param instantTime Instant Timestamp
   * @param n          Number of updates (including dups)
   * @return list of hoodie record updates
   */
  public List<HoodieRecord> generateUpdates(String instantTime, Integer n) throws IOException {
    List<HoodieRecord> updates = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      Map<Integer, KeyPartition> existingKeys = existingKeysBySchema.get(TRIP_EXAMPLE_SCHEMA);
      Integer numExistingKeys = numKeysBySchema.get(TRIP_EXAMPLE_SCHEMA);
      KeyPartition kp = existingKeys.get(RAND.nextInt(numExistingKeys - 1));
      HoodieRecord record = generateUpdateRecord(kp.key, instantTime);
      updates.add(record);
    }
    return updates;
  }

  /**
   * Generate update for each record in the dataset.
   * @param instantTime
   * @return
   * @throws IOException
   */
  public List<HoodieRecord> generateUpdatesForAllRecords(String instantTime) {
    List<HoodieRecord> updates = new ArrayList<>();
    Map<Integer, KeyPartition> existingKeys = existingKeysBySchema.get(TRIP_EXAMPLE_SCHEMA);
    existingKeys.values().forEach(kp -> {
      try {
        HoodieRecord record = generateUpdateRecord(kp.key, instantTime);
        updates.add(record);
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    });
    return updates;
  }

  public List<HoodieRecord> generateUpdatesAsPerSchema(String commitTime, Integer n, String schemaStr) {
    return generateUniqueUpdatesStream(commitTime, n, schemaStr).collect(Collectors.toList());
  }

  /**
   * Generates deduped updates of keys previously inserted, randomly distributed across the keys above.
   *
   * @param instantTime Instant Timestamp
   * @param n          Number of unique records
   * @return list of hoodie record updates
   */
  public List<HoodieRecord> generateUniqueUpdates(String instantTime, Integer n) {
    return generateUniqueUpdatesStream(instantTime, n, TRIP_EXAMPLE_SCHEMA).collect(Collectors.toList());
  }

  public List<HoodieRecord> generateUniqueUpdatesAsPerSchema(String instantTime, Integer n, String schemaStr) {
    return generateUniqueUpdatesStream(instantTime, n, schemaStr).collect(Collectors.toList());
  }

  /**
   * Generates deduped delete of keys previously inserted, randomly distributed across the keys above.
   *
   * @param n Number of unique records
   * @return list of hoodie record updates
   */
  public List<HoodieKey> generateUniqueDeletes(Integer n) {
    return generateUniqueDeleteStream(n).collect(Collectors.toList());
  }

  /**
   * Generates deduped updates of keys previously inserted, randomly distributed across the keys above.
   *
   * @param instantTime Commit Timestamp
   * @param n          Number of unique records
   * @return stream of hoodie record updates
   */
  public Stream<HoodieRecord> generateUniqueUpdatesStream(String instantTime, Integer n, String schemaStr) {
    final Set<KeyPartition> used = new HashSet<>();
    int numExistingKeys = numKeysBySchema.getOrDefault(schemaStr, 0);
    Map<Integer, KeyPartition> existingKeys = existingKeysBySchema.get(schemaStr);
    if (n > numExistingKeys) {
      throw new IllegalArgumentException("Requested unique updates is greater than number of available keys");
    }

    return IntStream.range(0, n).boxed().map(i -> {
      int index = numExistingKeys == 1 ? 0 : RAND.nextInt(numExistingKeys - 1);
      KeyPartition kp = existingKeys.get(index);
      // Find the available keyPartition starting from randomly chosen one.
      while (used.contains(kp)) {
        index = (index + 1) % numExistingKeys;
        kp = existingKeys.get(index);
      }
      logger.debug("key getting updated: " + kp.key.getRecordKey());
      used.add(kp);
      try {
        return new HoodieRecord(kp.key, generateRandomValueAsPerSchema(schemaStr, kp.key, instantTime, false));
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    });
  }

  /**
   * Generates deduped delete of keys previously inserted, randomly distributed across the keys above.
   *
   * @param n Number of unique records
   * @return stream of hoodie record updates
   */
  public Stream<HoodieKey> generateUniqueDeleteStream(Integer n) {
    final Set<KeyPartition> used = new HashSet<>();
    Map<Integer, KeyPartition> existingKeys = existingKeysBySchema.get(TRIP_EXAMPLE_SCHEMA);
    Integer numExistingKeys = numKeysBySchema.get(TRIP_EXAMPLE_SCHEMA);
    if (n > numExistingKeys) {
      throw new IllegalArgumentException("Requested unique deletes is greater than number of available keys");
    }

    List<HoodieKey> result = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      int index = RAND.nextInt(numExistingKeys);
      while (!existingKeys.containsKey(index)) {
        index = (index + 1) % numExistingKeys;
      }
      KeyPartition kp = existingKeys.remove(index);
      existingKeys.put(index, existingKeys.get(numExistingKeys - 1));
      existingKeys.remove(numExistingKeys - 1);
      numExistingKeys--;
      used.add(kp);
      result.add(kp.key);
    }
    numKeysBySchema.put(TRIP_EXAMPLE_SCHEMA, numExistingKeys);
    return result.stream();
  }

  /**
   * Generates deduped delete records previously inserted, randomly distributed across the keys above.
   *
   * @param instantTime Commit Timestamp
   * @param n          Number of unique records
   * @return stream of hoodie records for delete
   */
  public Stream<HoodieRecord> generateUniqueDeleteRecordStream(String instantTime, Integer n) {
    final Set<KeyPartition> used = new HashSet<>();
    Map<Integer, KeyPartition> existingKeys = existingKeysBySchema.get(TRIP_EXAMPLE_SCHEMA);
    Integer numExistingKeys = numKeysBySchema.get(TRIP_EXAMPLE_SCHEMA);
    if (n > numExistingKeys) {
      throw new IllegalArgumentException("Requested unique deletes is greater than number of available keys");
    }

    List<HoodieRecord> result = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      int index = RAND.nextInt(numExistingKeys);
      while (!existingKeys.containsKey(index)) {
        index = (index + 1) % numExistingKeys;
      }
      // swap chosen index with last index and remove last entry.
      KeyPartition kp = existingKeys.remove(index);
      existingKeys.put(index, existingKeys.get(numExistingKeys - 1));
      existingKeys.remove(numExistingKeys - 1);
      numExistingKeys--;
      used.add(kp);
      try {
        result.add(new HoodieRecord(kp.key, generateRandomDeleteValue(kp.key, instantTime)));
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    }
    numKeysBySchema.put(TRIP_EXAMPLE_SCHEMA, numExistingKeys);
    return result.stream();
  }

  /**
   * Generates deduped delete records previously inserted, randomly distributed across the keys above.
   *
   * @param instantTime Commit Timestamp
   * @param n          Number of unique records
   * @return List of hoodie records for delete
   */
  public List<HoodieRecord> generateUniqueDeleteRecords(String instantTime, Integer n) {
    return generateUniqueDeleteRecordStream(instantTime, n).collect(Collectors.toList());
  }

  public boolean deleteExistingKeyIfPresent(HoodieKey key) {
    Map<Integer, KeyPartition> existingKeys = existingKeysBySchema.get(TRIP_EXAMPLE_SCHEMA);
    Integer numExistingKeys = numKeysBySchema.get(TRIP_EXAMPLE_SCHEMA);
    for (Map.Entry<Integer, KeyPartition> entry : existingKeys.entrySet()) {
      if (entry.getValue().key.equals(key)) {
        int index = entry.getKey();
        existingKeys.put(index, existingKeys.get(numExistingKeys - 1));
        existingKeys.remove(numExistingKeys - 1);
        numExistingKeys--;
        numKeysBySchema.put(TRIP_EXAMPLE_SCHEMA, numExistingKeys);
        return true;
      }
    }
    return false;
  }

  public List<GenericRecord> generateGenericRecords(int numRecords) {
    List<GenericRecord> list = new ArrayList<>();
    IntStream.range(0, numRecords).forEach(i -> {
      list.add(generateGenericRecord(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID()
          .toString(), RAND.nextLong()));
    });
    return list;
  }

  public String[] getPartitionPaths() {
    return partitionPaths;
  }

  public int getNumExistingKeys(String schemaStr) {
    return numKeysBySchema.getOrDefault(schemaStr, 0);
  }

  public static class KeyPartition implements Serializable {

    HoodieKey key;
    String partitionPath;
  }

  public void close() {
    existingKeysBySchema.clear();
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utils;

import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.utils.StreamWriteFunctionWrapper;
import org.apache.hudi.table.HoodieFlinkTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Strings;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Data set for testing, also some utilities to check the results. */
public class TestData {
  public static List<RowData> DATA_SET_INSERT = Arrays.asList(
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
          TimestampData.fromEpochMillis(1), StringData.fromString("par1")),
      insertRow(StringData.fromString("id2"), StringData.fromString("Stephen"), 33,
          TimestampData.fromEpochMillis(2), StringData.fromString("par1")),
      insertRow(StringData.fromString("id3"), StringData.fromString("Julian"), 53,
          TimestampData.fromEpochMillis(3), StringData.fromString("par2")),
      insertRow(StringData.fromString("id4"), StringData.fromString("Fabian"), 31,
          TimestampData.fromEpochMillis(4), StringData.fromString("par2")),
      insertRow(StringData.fromString("id5"), StringData.fromString("Sophia"), 18,
          TimestampData.fromEpochMillis(5), StringData.fromString("par3")),
      insertRow(StringData.fromString("id6"), StringData.fromString("Emma"), 20,
          TimestampData.fromEpochMillis(6), StringData.fromString("par3")),
      insertRow(StringData.fromString("id7"), StringData.fromString("Bob"), 44,
          TimestampData.fromEpochMillis(7), StringData.fromString("par4")),
      insertRow(StringData.fromString("id8"), StringData.fromString("Han"), 56,
          TimestampData.fromEpochMillis(8), StringData.fromString("par4"))
  );

  public static List<RowData> DATA_SET_UPDATE_INSERT = Arrays.asList(
      // advance the age by 1
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 24,
          TimestampData.fromEpochMillis(1), StringData.fromString("par1")),
      insertRow(StringData.fromString("id2"), StringData.fromString("Stephen"), 34,
          TimestampData.fromEpochMillis(2), StringData.fromString("par1")),
      insertRow(StringData.fromString("id3"), StringData.fromString("Julian"), 54,
          TimestampData.fromEpochMillis(3), StringData.fromString("par2")),
      insertRow(StringData.fromString("id4"), StringData.fromString("Fabian"), 32,
          TimestampData.fromEpochMillis(4), StringData.fromString("par2")),
      // same with before
      insertRow(StringData.fromString("id5"), StringData.fromString("Sophia"), 18,
          TimestampData.fromEpochMillis(5), StringData.fromString("par3")),
      // new data
      insertRow(StringData.fromString("id9"), StringData.fromString("Jane"), 19,
          TimestampData.fromEpochMillis(6), StringData.fromString("par3")),
      insertRow(StringData.fromString("id10"), StringData.fromString("Ella"), 38,
          TimestampData.fromEpochMillis(7), StringData.fromString("par4")),
      insertRow(StringData.fromString("id11"), StringData.fromString("Phoebe"), 52,
          TimestampData.fromEpochMillis(8), StringData.fromString("par4"))
  );

  public static List<RowData> DATA_SET_INSERT_SEPARATE_PARTITION = Arrays.asList(
      insertRow(StringData.fromString("id12"), StringData.fromString("Monica"), 27,
          TimestampData.fromEpochMillis(9), StringData.fromString("par5")),
      insertRow(StringData.fromString("id13"), StringData.fromString("Phoebe"), 31,
          TimestampData.fromEpochMillis(10), StringData.fromString("par5")),
      insertRow(StringData.fromString("id14"), StringData.fromString("Rachel"), 52,
          TimestampData.fromEpochMillis(11), StringData.fromString("par6")),
      insertRow(StringData.fromString("id15"), StringData.fromString("Ross"), 29,
          TimestampData.fromEpochMillis(12), StringData.fromString("par6"))
  );

  public static List<RowData> DATA_SET_INSERT_DUPLICATES = new ArrayList<>();
  static {
    IntStream.range(0, 5).forEach(i -> DATA_SET_INSERT_DUPLICATES.add(
        insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
            TimestampData.fromEpochMillis(1), StringData.fromString("par1"))));
  }

  public static List<RowData> DATA_SET_INSERT_SAME_KEY = new ArrayList<>();
  static {
    IntStream.range(0, 5).forEach(i -> DATA_SET_INSERT_SAME_KEY.add(
        insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
            TimestampData.fromEpochMillis(i), StringData.fromString("par1"))));
  }

  // data set of test_source.data
  public static List<RowData> DATA_SET_SOURCE_INSERT = Arrays.asList(
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
          TimestampData.fromEpochMillis(1000), StringData.fromString("par1")),
      insertRow(StringData.fromString("id2"), StringData.fromString("Stephen"), 33,
          TimestampData.fromEpochMillis(2000), StringData.fromString("par1")),
      insertRow(StringData.fromString("id3"), StringData.fromString("Julian"), 53,
          TimestampData.fromEpochMillis(3000), StringData.fromString("par2")),
      insertRow(StringData.fromString("id4"), StringData.fromString("Fabian"), 31,
          TimestampData.fromEpochMillis(4000), StringData.fromString("par2")),
      insertRow(StringData.fromString("id5"), StringData.fromString("Sophia"), 18,
          TimestampData.fromEpochMillis(5000), StringData.fromString("par3")),
      insertRow(StringData.fromString("id6"), StringData.fromString("Emma"), 20,
          TimestampData.fromEpochMillis(6000), StringData.fromString("par3")),
      insertRow(StringData.fromString("id7"), StringData.fromString("Bob"), 44,
          TimestampData.fromEpochMillis(7000), StringData.fromString("par4")),
      insertRow(StringData.fromString("id8"), StringData.fromString("Han"), 56,
          TimestampData.fromEpochMillis(8000), StringData.fromString("par4"))
  );

  // merged data set of test_source.data and test_source_2.data
  public static List<RowData> DATA_SET_SOURCE_MERGED = Arrays.asList(
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 24,
          TimestampData.fromEpochMillis(1000), StringData.fromString("par1")),
      insertRow(StringData.fromString("id2"), StringData.fromString("Stephen"), 34,
          TimestampData.fromEpochMillis(2000), StringData.fromString("par1")),
      insertRow(StringData.fromString("id3"), StringData.fromString("Julian"), 54,
          TimestampData.fromEpochMillis(3000), StringData.fromString("par2")),
      insertRow(StringData.fromString("id4"), StringData.fromString("Fabian"), 32,
          TimestampData.fromEpochMillis(4000), StringData.fromString("par2")),
      insertRow(StringData.fromString("id5"), StringData.fromString("Sophia"), 18,
          TimestampData.fromEpochMillis(5000), StringData.fromString("par3")),
      insertRow(StringData.fromString("id6"), StringData.fromString("Emma"), 20,
          TimestampData.fromEpochMillis(6000), StringData.fromString("par3")),
      insertRow(StringData.fromString("id7"), StringData.fromString("Bob"), 44,
          TimestampData.fromEpochMillis(7000), StringData.fromString("par4")),
      insertRow(StringData.fromString("id8"), StringData.fromString("Han"), 56,
          TimestampData.fromEpochMillis(8000), StringData.fromString("par4")),
      insertRow(StringData.fromString("id9"), StringData.fromString("Jane"), 19,
          TimestampData.fromEpochMillis(6000), StringData.fromString("par3")),
      insertRow(StringData.fromString("id10"), StringData.fromString("Ella"), 38,
          TimestampData.fromEpochMillis(7000), StringData.fromString("par4")),
      insertRow(StringData.fromString("id11"), StringData.fromString("Phoebe"), 52,
          TimestampData.fromEpochMillis(8000), StringData.fromString("par4"))
  );

  // data set of test_source.data with partition 'par1' overwrite
  public static List<RowData> DATA_SET_SOURCE_INSERT_OVERWRITE = Arrays.asList(
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 24,
          TimestampData.fromEpochMillis(1000), StringData.fromString("par1")),
      insertRow(StringData.fromString("id2"), StringData.fromString("Stephen"), 34,
          TimestampData.fromEpochMillis(2000), StringData.fromString("par1")),
      insertRow(StringData.fromString("id3"), StringData.fromString("Julian"), 53,
          TimestampData.fromEpochMillis(3000), StringData.fromString("par2")),
      insertRow(StringData.fromString("id4"), StringData.fromString("Fabian"), 31,
          TimestampData.fromEpochMillis(4000), StringData.fromString("par2")),
      insertRow(StringData.fromString("id5"), StringData.fromString("Sophia"), 18,
          TimestampData.fromEpochMillis(5000), StringData.fromString("par3")),
      insertRow(StringData.fromString("id6"), StringData.fromString("Emma"), 20,
          TimestampData.fromEpochMillis(6000), StringData.fromString("par3")),
      insertRow(StringData.fromString("id7"), StringData.fromString("Bob"), 44,
          TimestampData.fromEpochMillis(7000), StringData.fromString("par4")),
      insertRow(StringData.fromString("id8"), StringData.fromString("Han"), 56,
          TimestampData.fromEpochMillis(8000), StringData.fromString("par4"))
  );

  public static List<RowData> DATA_SET_UPDATE_DELETE = Arrays.asList(
      // this is update
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 24,
          TimestampData.fromEpochMillis(1), StringData.fromString("par1")),
      insertRow(StringData.fromString("id2"), StringData.fromString("Stephen"), 34,
          TimestampData.fromEpochMillis(2), StringData.fromString("par1")),
      // this is delete
      deleteRow(StringData.fromString("id3"), StringData.fromString("Julian"), 53,
          TimestampData.fromEpochMillis(3), StringData.fromString("par2")),
      deleteRow(StringData.fromString("id5"), StringData.fromString("Sophia"), 18,
          TimestampData.fromEpochMillis(5), StringData.fromString("par3")),
      // delete a record that has no inserts
      deleteRow(StringData.fromString("id9"), StringData.fromString("Jane"), 19,
          TimestampData.fromEpochMillis(6), StringData.fromString("par3"))
  );

  /**
   * Returns string format of a list of RowData.
   */
  public static String rowDataToString(List<RowData> rows) {
    DataStructureConverter<Object, Object> converter =
        DataStructureConverters.getConverter(TestConfigurations.ROW_DATA_TYPE);
    return rows.stream()
        .map(row -> converter.toExternal(row).toString())
        .sorted(Comparator.naturalOrder())
        .collect(Collectors.toList()).toString();
  }

  /**
   * Write a list of row data with Hoodie format base on the given configuration.
   *
   * @param dataBuffer  The data buffer to write
   * @param conf        The flink configuration
   * @throws Exception if error occurs
   */
  public static void writeData(
      List<RowData> dataBuffer,
      Configuration conf) throws Exception {
    StreamWriteFunctionWrapper<RowData> funcWrapper = new StreamWriteFunctionWrapper<>(
        conf.getString(FlinkOptions.PATH),
        conf);
    funcWrapper.openFunction();

    for (RowData rowData : dataBuffer) {
      funcWrapper.invoke(rowData);
    }

    // this triggers the data write and event send
    funcWrapper.checkpointFunction(1);

    final OperatorEvent nextEvent = funcWrapper.getNextEvent();
    funcWrapper.getCoordinator().handleEventFromOperator(0, nextEvent);
    funcWrapper.checkpointComplete(1);

    funcWrapper.close();
  }

  private static String toStringSafely(Object obj) {
    return obj == null ? "null" : obj.toString();
  }

  /**
   * Sort the {@code rows} using field at index 0 and asserts
   * it equals with the expected string {@code expected}.
   *
   * @param rows     Actual result rows
   * @param expected Expected string of the sorted rows
   */
  public static void assertRowsEquals(List<Row> rows, String expected) {
    assertRowsEquals(rows, expected, 0);
  }

  /**
   * Sort the {@code rows} using field at index {@code orderingPos} and asserts
   * it equals with the expected string {@code expected}.
   *
   * @param rows     Actual result rows
   * @param expected Expected string of the sorted rows
   * @param orderingPos Field position for ordering
   */
  public static void assertRowsEquals(List<Row> rows, String expected, int orderingPos) {
    String rowsString = rows.stream()
        .sorted(Comparator.comparing(o -> toStringSafely(o.getField(orderingPos))))
        .collect(Collectors.toList()).toString();
    assertThat(rowsString, is(expected));
  }

  /**
   * Sort the {@code rows} using field at index 0 and asserts
   * it equals with the expected row data list {@code expected}.
   *
   * @param rows     Actual result rows
   * @param expected Expected row data list
   */
  public static void assertRowsEquals(List<Row> rows, List<RowData> expected) {
    String rowsString = rows.stream()
        .sorted(Comparator.comparing(o -> toStringSafely(o.getField(0))))
        .collect(Collectors.toList()).toString();
    assertThat(rowsString, is(rowDataToString(expected)));
  }

  /**
   * Sort the {@code rows} using field at index {@code orderingPos} and asserts
   * it equals with the expected string {@code expected}.
   *
   * @param rows     Actual result rows
   * @param contains contains string of the sorted rows
   * @param orderingPos Field position for ordering
   */
  public static void assertRowsContains(List<Row> rows, List<String> contains, int orderingPos) {
    String rowsString = rows.stream()
        .sorted(Comparator.comparing(o -> toStringSafely(o.getField(orderingPos))))
        .collect(Collectors.toList()).toString();

    contains.forEach(s -> assertThat(rowsString, containsString(s)));
  }

  /**
   * Sort the {@code rows} using field at index 0 and asserts
   * it equals with the expected string {@code expected}.
   *
   * @param rows     Actual result rows
   * @param expected Expected string of the sorted rows
   */
  public static void assertRowDataEquals(List<RowData> rows, String expected) {
    String rowsString = rowDataToString(rows);
    assertThat(rowsString, is(expected));
  }

  /**
   * Sort the {@code rows} using field at index 0 and asserts
   * it equals with the expected row data list {@code expected}.
   *
   * @param rows     Actual result rows
   * @param expected Expected row data list
   */
  public static void assertRowDataEquals(List<RowData> rows, List<RowData> expected) {
    String rowsString = rowDataToString(rows);
    assertThat(rowsString, is(rowDataToString(expected)));
  }

  /**
   * Checks the source data set are written as expected.
   *
   * <p>Note: Replace it with the Flink reader when it is supported.
   *
   * @param baseFile The file base to check, should be a directory
   * @param expected The expected results mapping, the key should be the partition path
   *                 and value should be values list with the key partition
   */
  public static void checkWrittenData(File baseFile, Map<String, String> expected) throws IOException {
    checkWrittenData(baseFile, expected, 4);
  }

  /**
   * Checks the source data set are written as expected.
   *
   * <p>Note: Replace it with the Flink reader when it is supported.
   *
   * @param baseFile   The file base to check, should be a directory
   * @param expected   The expected results mapping, the key should be the partition path
   *                   and value should be values list with the key partition
   * @param partitions The expected partition number
   */
  public static void checkWrittenData(
      File baseFile,
      Map<String, String> expected,
      int partitions) throws IOException {
    assert baseFile.isDirectory();
    FileFilter filter = file -> !file.getName().startsWith(".");
    File[] partitionDirs = baseFile.listFiles(filter);

    assertNotNull(partitionDirs);
    assertThat(partitionDirs.length, is(partitions));

    for (File partitionDir : partitionDirs) {
      File[] dataFiles = partitionDir.listFiles(filter);
      assertNotNull(dataFiles);
      File latestDataFile = Arrays.stream(dataFiles)
          .max(Comparator.comparing(f -> FSUtils.getCommitTime(f.getName())))
          .orElse(dataFiles[0]);
      ParquetReader<GenericRecord> reader = AvroParquetReader
          .<GenericRecord>builder(new Path(latestDataFile.getAbsolutePath())).build();
      List<String> readBuffer = new ArrayList<>();
      GenericRecord nextRecord = reader.read();
      while (nextRecord != null) {
        readBuffer.add(filterOutVariables(nextRecord));
        nextRecord = reader.read();
      }
      readBuffer.sort(Comparator.naturalOrder());
      assertThat(readBuffer.toString(), is(expected.get(partitionDir.getName())));
    }
  }

  public static void checkWrittenAllData(
      File baseFile,
      Map<String, List<String>> expected,
      int partitions) throws IOException {
    assert baseFile.isDirectory();
    FileFilter filter = file -> !file.getName().startsWith(".");
    File[] partitionDirs = baseFile.listFiles(filter);

    assertNotNull(partitionDirs);
    assertThat(partitionDirs.length, is(partitions));

    for (File partitionDir : partitionDirs) {
      File[] dataFiles = partitionDir.listFiles(filter);
      assertNotNull(dataFiles);

      List<String> readBuffer = new ArrayList<>();
      for (File dataFile : dataFiles) {
        ParquetReader<GenericRecord> reader = AvroParquetReader
            .<GenericRecord>builder(new Path(dataFile.getAbsolutePath())).build();
        GenericRecord nextRecord = reader.read();
        while (nextRecord != null) {
          readBuffer.add(filterOutVariables(nextRecord));
          nextRecord = reader.read();
        }
        readBuffer.sort(Comparator.naturalOrder());
      }

      assertThat(readBuffer, is(expected.get(partitionDir.getName())));
    }
  }

  /**
   * Checks the source data are written as expected.
   *
   * <p>Note: Replace it with the Flink reader when it is supported.
   *
   * @param basePath   The file base to check, should be a directory
   * @param expected   The expected results mapping, the key should be the partition path
   */
  public static void checkWrittenFullData(
      File basePath,
      Map<String, List<String>> expected) throws IOException {

    // 1. init flink table
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath.getAbsolutePath());
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath.getAbsolutePath()).build();
    FlinkTaskContextSupplier supplier = new FlinkTaskContextSupplier(null);
    HoodieFlinkEngineContext context = new HoodieFlinkEngineContext(supplier);
    HoodieFlinkTable table = HoodieFlinkTable.create(config, context, metaClient);

    // 2. check each partition data
    expected.forEach((partition, partitionDataSet) -> {

      List<String> readBuffer = new ArrayList<>();

      table.getFileSystemView().getAllFileGroups(partition)
          .forEach(v -> v.getLatestDataFile().ifPresent(baseFile -> {
            String path = baseFile.getPath();
            try {
              ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(path)).build();
              GenericRecord nextRecord = reader.read();
              while (nextRecord != null) {
                readBuffer.add(filterOutVariables(nextRecord));
                nextRecord = reader.read();
              }
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }));

      assertTrue(partitionDataSet.size() == readBuffer.size() && partitionDataSet.containsAll(readBuffer));

    });

  }

  /**
   * Checks the MERGE_ON_READ source data are written as expected.
   *
   * <p>Note: Replace it with the Flink reader when it is supported.
   *
   * @param fs         The file system
   * @param latestInstant The latest committed instant of current table
   * @param baseFile   The file base to check, should be a directory
   * @param expected   The expected results mapping, the key should be the partition path
   * @param partitions The expected partition number
   * @param schema     The read schema
   */
  public static void checkWrittenDataMOR(
      FileSystem fs,
      String latestInstant,
      File baseFile,
      Map<String, String> expected,
      int partitions,
      Schema schema) {
    assert baseFile.isDirectory() : "Base path should be a directory";
    FileFilter partitionFilter = file -> !file.getName().startsWith(".");
    File[] partitionDirs = baseFile.listFiles(partitionFilter);
    assertNotNull(partitionDirs);
    assertThat(partitionDirs.length, is(partitions));
    for (File partitionDir : partitionDirs) {
      File[] dataFiles = partitionDir.listFiles(file ->
          file.getName().contains(".log.") && !file.getName().startsWith(".."));
      assertNotNull(dataFiles);
      HoodieMergedLogRecordScanner scanner = getScanner(
          fs, baseFile.getPath(), Arrays.stream(dataFiles).map(File::getAbsolutePath)
              .sorted(Comparator.naturalOrder()).collect(Collectors.toList()),
          schema, latestInstant);
      List<String> readBuffer = scanner.getRecords().values().stream()
          .map(hoodieRecord -> {
            try {
              // in case it is a delete
              GenericRecord record = (GenericRecord) hoodieRecord.getData()
                  .getInsertValue(schema, new Properties())
                  .orElse(null);
              return record == null ? (String) null : filterOutVariables(record);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          })
          .filter(Objects::nonNull)
          .sorted(Comparator.naturalOrder())
          .collect(Collectors.toList());
      assertThat(readBuffer.toString(), is(expected.get(partitionDir.getName())));
    }
  }

  /**
   * Returns the scanner to read avro log files.
   */
  private static HoodieMergedLogRecordScanner getScanner(
      FileSystem fs,
      String basePath,
      List<String> logPaths,
      Schema readSchema,
      String instant) {
    return HoodieMergedLogRecordScanner.newBuilder()
        .withFileSystem(fs)
        .withBasePath(basePath)
        .withLogFilePaths(logPaths)
        .withReaderSchema(readSchema)
        .withLatestInstantTime(instant)
        .withReadBlocksLazily(false)
        .withReverseReader(false)
        .withBufferSize(16 * 1024 * 1024)
        .withMaxMemorySizeInBytes(1024 * 1024L)
        .withSpillableMapBasePath("/tmp/")
        .build();
  }

  /**
   * Filter out the variables like file name.
   */
  private static String filterOutVariables(GenericRecord genericRecord) {
    List<String> fields = new ArrayList<>();
    fields.add(genericRecord.get("_hoodie_record_key").toString());
    fields.add(genericRecord.get("_hoodie_partition_path").toString());
    fields.add(genericRecord.get("uuid").toString());
    fields.add(genericRecord.get("name").toString());
    fields.add(genericRecord.get("age").toString());
    fields.add(genericRecord.get("ts").toString());
    fields.add(genericRecord.get("partition").toString());
    return Strings.join(fields, ",");
  }

  private static BinaryRowData insertRow(Object... fields) {
    LogicalType[] types = TestConfigurations.ROW_TYPE.getFields().stream().map(RowType.RowField::getType)
        .toArray(LogicalType[]::new);
    assertEquals(
        "Filed count inconsistent with type information",
        fields.length,
        types.length);
    BinaryRowData row = new BinaryRowData(fields.length);
    BinaryRowWriter writer = new BinaryRowWriter(row);
    writer.reset();
    for (int i = 0; i < fields.length; i++) {
      Object field = fields[i];
      if (field == null) {
        writer.setNullAt(i);
      } else {
        BinaryWriter.write(writer, i, field, types[i], InternalSerializers.create(types[i]));
      }
    }
    writer.complete();
    return row;
  }

  private static BinaryRowData deleteRow(Object... fields) {
    BinaryRowData rowData = insertRow(fields);
    rowData.setRowKind(RowKind.DELETE);
    return rowData;
  }
}

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

package org.apache.hudi.keygen;

import org.apache.hudi.AvroConversionHelper;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.exception.HoodieKeyGeneratorException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;

import scala.Function1;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestTimestampBasedKeyGenerator {

  private GenericRecord baseRecord;
  private TypedProperties properties = new TypedProperties();

  private Schema schema;
  private StructType structType;
  private Row baseRow;

  @BeforeEach
  public void initialize() throws IOException {
    schema = SchemaTestUtil.getTimestampEvolvedSchema();
    structType = AvroConversionUtils.convertAvroSchemaToStructType(schema);
    baseRecord = SchemaTestUtil
        .generateAvroRecordFromJson(schema, 1, "001", "f1");
    baseRow = genericRecordToRow(baseRecord);

    properties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY.key(), "field1");
    properties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY.key(), "createTime");
    properties.setProperty(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_OPT_KEY.key(), "false");
  }

  private TypedProperties getBaseKeyConfig(String timestampType, String dateFormat, String timezone, String scalarType) {
    properties.setProperty(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_TYPE_FIELD_PROP, timestampType);
    properties.setProperty(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, dateFormat);
    properties.setProperty(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_TIMEZONE_FORMAT_PROP, timezone);

    if (scalarType != null) {
      properties.setProperty("hoodie.deltastreamer.keygen.timebased.timestamp.scalar.time.unit", scalarType);
    }

    return properties;
  }

  private Row genericRecordToRow(GenericRecord baseRecord) {
    Function1<Object, Object> convertor = AvroConversionHelper.createConverterToRow(schema, structType);
    Row row = (Row) convertor.apply(baseRecord);
    int fieldCount = structType.fieldNames().length;
    Object[] values = new Object[fieldCount];
    for (int i = 0; i < fieldCount; i++) {
      values[i] = row.get(i);
    }
    return new GenericRowWithSchema(values, structType);
  }

  private TypedProperties getBaseKeyConfig(String timestampType, String inputFormatList, String inputFormatDelimiterRegex, String inputTimezone, String outputFormat, String outputTimezone) {
    if (timestampType != null) {
      properties.setProperty(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_TYPE_FIELD_PROP, timestampType);
    }
    if (inputFormatList != null) {
      properties.setProperty(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP, inputFormatList);
    }
    if (inputFormatDelimiterRegex != null) {
      properties.setProperty(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_INPUT_DATE_FORMAT_LIST_DELIMITER_REGEX_PROP, inputFormatDelimiterRegex);
    }
    if (inputTimezone != null) {
      properties.setProperty(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_INPUT_TIMEZONE_FORMAT_PROP, inputTimezone);
    }
    if (outputFormat != null) {
      properties.setProperty(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, outputFormat);
    }
    if (outputTimezone != null) {
      properties.setProperty(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_OUTPUT_TIMEZONE_FORMAT_PROP, outputTimezone);
    }
    return properties;
  }

  @Test
  public void testTimestampBasedKeyGenerator() throws IOException {
    // timezone is GMT+8:00
    baseRecord.put("createTime", 1578283932000L);
    properties = getBaseKeyConfig("EPOCHMILLISECONDS", "yyyy-MM-dd hh", "GMT+8:00", null);
    TimestampBasedKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    assertEquals("2020-01-06 12", hk1.getPartitionPath());

    // timezone is GMT+8:00, createTime is BigDecimal
    baseRecord.put("createTime", new BigDecimal(1578283932000.00001));
    properties = getBaseKeyConfig("EPOCHMILLISECONDS", "yyyy-MM-dd hh", "GMT+8:00", null);
    keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey bigDecimalKey = keyGen.getKey(baseRecord);
    assertEquals("2020-01-06 12", bigDecimalKey.getPartitionPath());

    // test w/ Row
    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020-01-06 12", keyGen.getPartitionPath(baseRow));

    // timezone is GMT
    properties = getBaseKeyConfig("EPOCHMILLISECONDS", "yyyy-MM-dd hh", "GMT", null);
    keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk2 = keyGen.getKey(baseRecord);
    assertEquals("2020-01-06 04", hk2.getPartitionPath());

    // test w/ Row
    assertEquals("2020-01-06 04", keyGen.getPartitionPath(baseRow));

    // timestamp is DATE_STRING, timezone is GMT+8:00
    baseRecord.put("createTime", "2020-01-06 12:12:12");
    properties = getBaseKeyConfig("DATE_STRING", "yyyy-MM-dd hh", "GMT+8:00", null);
    properties.setProperty("hoodie.deltastreamer.keygen.timebased.input.dateformat", "yyyy-MM-dd hh:mm:ss");
    keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk3 = keyGen.getKey(baseRecord);
    assertEquals("2020-01-06 12", hk3.getPartitionPath());

    // test w/ Row
    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020-01-06 12", keyGen.getPartitionPath(baseRow));

    // timezone is GMT
    properties = getBaseKeyConfig("DATE_STRING", "yyyy-MM-dd hh", "GMT", null);
    keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk4 = keyGen.getKey(baseRecord);
    assertEquals("2020-01-06 12", hk4.getPartitionPath());

    // test w/ Row
    assertEquals("2020-01-06 12", keyGen.getPartitionPath(baseRow));

    // timezone is GMT+8:00, createTime is null
    baseRecord.put("createTime", null);
    properties = getBaseKeyConfig("EPOCHMILLISECONDS", "yyyy-MM-dd hh", "GMT+8:00", null);
    keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk5 = keyGen.getKey(baseRecord);
    assertEquals("1970-01-01 08", hk5.getPartitionPath());

    // test w/ Row
    baseRow = genericRecordToRow(baseRecord);
    assertEquals("1970-01-01 08", keyGen.getPartitionPath(baseRow));

    // timestamp is DATE_STRING, timezone is GMT, createTime is null
    baseRecord.put("createTime", null);
    properties = getBaseKeyConfig("DATE_STRING", "yyyy-MM-dd hh:mm:ss", "GMT", null);
    properties.setProperty("hoodie.deltastreamer.keygen.timebased.input.dateformat", "yyyy-MM-dd hh:mm:ss");
    keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk6 = keyGen.getKey(baseRecord);
    assertEquals("1970-01-01 12:00:00", hk6.getPartitionPath());

    // test w/ Row
    baseRow = genericRecordToRow(baseRecord);
    assertEquals("1970-01-01 12:00:00", keyGen.getPartitionPath(baseRow));
  }

  @Test
  public void testScalar() throws IOException {
    // timezone is GMT+8:00
    baseRecord.put("createTime", 20000L);

    // timezone is GMT
    properties = getBaseKeyConfig("SCALAR", "yyyy-MM-dd hh", "GMT", "days");
    TimestampBasedKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    assertEquals(hk1.getPartitionPath(), "2024-10-04 12");

    // test w/ Row
    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2024-10-04 12", keyGen.getPartitionPath(baseRow));

    // timezone is GMT, createTime is null
    baseRecord.put("createTime", null);
    properties = getBaseKeyConfig("SCALAR", "yyyy-MM-dd hh", "GMT", "days");
    keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk2 = keyGen.getKey(baseRecord);
    assertEquals("1970-01-02 12", hk2.getPartitionPath());

    // test w/ Row
    baseRow = genericRecordToRow(baseRecord);
    assertEquals("1970-01-02 12", keyGen.getPartitionPath(baseRow));

    // timezone is GMT. number of days store integer in mysql
    baseRecord.put("createTime", 18736);
    properties = getBaseKeyConfig("SCALAR", "yyyy-MM-dd", "GMT", "DAYS");
    keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey scalarSecondsKey = keyGen.getKey(baseRecord);
    assertEquals("2021-04-19", scalarSecondsKey.getPartitionPath());

  }

  @Test
  public void test_ExpectsMatch_SingleInputFormat_ISO8601WithMsZ_OutputTimezoneAsUTC() throws IOException {
    baseRecord.put("createTime", "2020-04-01T13:01:33.428Z");
    properties = this.getBaseKeyConfig(
        "DATE_STRING",
        "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
        "",
        "",
        "yyyyMMddHH",
        "GMT");
    BuiltinKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    Assertions.assertEquals("2020040113", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020040113", keyGen.getPartitionPath(baseRow));
  }

  @Test
  public void test_ExpectsMatch_SingleInputFormats_ISO8601WithMsZ_OutputTimezoneAsInputDateTimeZone() throws IOException {
    baseRecord.put("createTime", "2020-04-01T13:01:33.428Z");
    properties = this.getBaseKeyConfig(
        "DATE_STRING",
        "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
        "",
        "",
        "yyyyMMddHH",
        "");
    BuiltinKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    Assertions.assertEquals("2020040113", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020040113", keyGen.getPartitionPath(baseRow));
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ISO8601WithMsZ_OutputTimezoneAsUTC() throws IOException {
    baseRecord.put("createTime", "2020-04-01T13:01:33.428Z");
    properties = this.getBaseKeyConfig(
        "DATE_STRING",
        "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
        "",
        "",
        "yyyyMMddHH",
        "UTC");
    BuiltinKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    Assertions.assertEquals("2020040113", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020040113", keyGen.getPartitionPath(baseRow));
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ISO8601NoMsZ_OutputTimezoneAsUTC() throws IOException {
    baseRecord.put("createTime", "2020-04-01T13:01:33Z");
    properties = this.getBaseKeyConfig(
        "DATE_STRING",
        "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
        "",
        "",
        "yyyyMMddHH",
        "UTC");
    BuiltinKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    Assertions.assertEquals("2020040113", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020040113", keyGen.getPartitionPath(baseRow));
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ISO8601NoMsWithOffset_OutputTimezoneAsUTC() throws IOException {
    baseRecord.put("createTime", "2020-04-01T13:01:33-05:00");
    properties = this.getBaseKeyConfig(
        "DATE_STRING",
        "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
        "",
        "",
        "yyyyMMddHH",
        "UTC");
    BuiltinKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    Assertions.assertEquals("2020040118", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020040118", keyGen.getPartitionPath(baseRow));
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ISO8601WithMsWithOffset_OutputTimezoneAsUTC() throws IOException {
    baseRecord.put("createTime", "2020-04-01T13:01:33.123-05:00");
    properties = this.getBaseKeyConfig(
        "DATE_STRING",
        "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
        "",
        "",
        "yyyyMMddHH",
        "UTC");
    BuiltinKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    Assertions.assertEquals("2020040118", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020040118", keyGen.getPartitionPath(baseRow));
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ISO8601WithMsZ_OutputTimezoneAsEST() throws IOException {
    baseRecord.put("createTime", "2020-04-01T13:01:33.123Z");
    properties = this.getBaseKeyConfig(
        "DATE_STRING",
        "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
        "",
        "",
        "yyyyMMddHH",
        "EST");
    BuiltinKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    Assertions.assertEquals("2020040109", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020040109", keyGen.getPartitionPath(baseRow));
  }

  @Test
  public void test_Throws_MultipleInputFormats_InputDateNotMatchingFormats() throws IOException {
    baseRecord.put("createTime", "2020-04-01 13:01:33.123-05:00");
    properties = this.getBaseKeyConfig(
        "DATE_STRING",
        "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
        "",
        "",
        "yyyyMMddHH",
        "UTC");
    BuiltinKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    Assertions.assertThrows(HoodieKeyGeneratorException.class, () -> keyGen.getKey(baseRecord));

    baseRow = genericRecordToRow(baseRecord);
    Assertions.assertThrows(HoodieKeyGeneratorException.class, () -> keyGen.getPartitionPath(baseRow));
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ShortDate_OutputCustomDate() throws IOException {
    baseRecord.put("createTime", "20200401");
    properties = this.getBaseKeyConfig(
        "DATE_STRING",
        "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ,yyyyMMdd",
        "",
        "UTC",
        "MM/dd/yyyy",
        "UTC");
    BuiltinKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    Assertions.assertEquals("04/01/2020", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("04/01/2020", keyGen.getPartitionPath(baseRow));
  }
}

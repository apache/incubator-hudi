/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.keygen;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieKeyGeneratorException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.parser.AbstractHoodieDateTimeParser;
import org.apache.hudi.keygen.parser.HoodieDateTimeParserImpl;

import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Avro Key generator, that relies on timestamps for partitioning field. Still picks record key by name.
 */
public class TimestampBasedAvroKeyGenerator extends SimpleAvroKeyGenerator {
  public enum TimestampType implements Serializable {
    UNIX_TIMESTAMP, DATE_STRING, MIXED, EPOCHMILLISECONDS, SCALAR
  }

  private final TimeUnit timeUnit;
  private final TimestampType timestampType;
  private final String outputDateFormat;
  private transient Option<DateTimeFormatter> inputFormatter;
  private transient DateTimeFormatter partitionFormatter;
  private final AbstractHoodieDateTimeParser parser;

  private final ZoneId inputDateTimeZone;
  private final ZoneId outputDateTimeZone;

  protected final boolean encodePartitionPath;

  /**
   * Supported configs.
   */
  public static class Config {

    // One value from TimestampType above
    public static final String TIMESTAMP_TYPE_FIELD_PROP = "hoodie.deltastreamer.keygen.timebased.timestamp.type";
    public static final String INPUT_TIME_UNIT =
        "hoodie.deltastreamer.keygen.timebased.timestamp.scalar.time.unit";
    //This prop can now accept list of input date formats.
    public static final String TIMESTAMP_INPUT_DATE_FORMAT_PROP =
        "hoodie.deltastreamer.keygen.timebased.input.dateformat";
    public static final String TIMESTAMP_INPUT_DATE_FORMAT_LIST_DELIMITER_REGEX_PROP = "hoodie.deltastreamer.keygen.timebased.input.dateformat.list.delimiter.regex";
    public static final String TIMESTAMP_INPUT_TIMEZONE_FORMAT_PROP = "hoodie.deltastreamer.keygen.timebased.input.timezone";
    public static final String TIMESTAMP_OUTPUT_DATE_FORMAT_PROP =
        "hoodie.deltastreamer.keygen.timebased.output.dateformat";
    //still keeping this prop for backward compatibility so that functionality for existing users does not break.
    public static final String TIMESTAMP_TIMEZONE_FORMAT_PROP =
        "hoodie.deltastreamer.keygen.timebased.timezone";
    public static final String TIMESTAMP_OUTPUT_TIMEZONE_FORMAT_PROP = "hoodie.deltastreamer.keygen.timebased.output.timezone";
    static final String DATE_TIME_PARSER_PROP = "hoodie.deltastreamer.keygen.datetime.parser.class";
  }

  public TimestampBasedAvroKeyGenerator(TypedProperties config) throws IOException {
    this(config, config.getString(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY.key()),
        config.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY.key()));
  }

  TimestampBasedAvroKeyGenerator(TypedProperties config, String partitionPathField) throws IOException {
    this(config, null, partitionPathField);
  }

  TimestampBasedAvroKeyGenerator(TypedProperties config, String recordKeyField, String partitionPathField) throws IOException {
    super(config, recordKeyField, partitionPathField);
    String dateTimeParserClass = config.getString(Config.DATE_TIME_PARSER_PROP, HoodieDateTimeParserImpl.class.getName());
    this.parser = KeyGenUtils.createDateTimeParser(config, dateTimeParserClass);
    this.inputDateTimeZone = parser.getInputDateTimeZone();
    this.outputDateTimeZone = parser.getOutputDateTimeZone();
    this.outputDateFormat = parser.getOutputDateFormat();
    this.timestampType = TimestampType.valueOf(config.getString(Config.TIMESTAMP_TYPE_FIELD_PROP));

    switch (this.timestampType) {
      case EPOCHMILLISECONDS:
        timeUnit = MILLISECONDS;
        break;
      case UNIX_TIMESTAMP:
        timeUnit = SECONDS;
        break;
      case SCALAR:
        String timeUnitStr = config.getString(Config.INPUT_TIME_UNIT, TimeUnit.SECONDS.toString());
        timeUnit = TimeUnit.valueOf(timeUnitStr.toUpperCase());
        break;
      default:
        timeUnit = null;
    }
    this.encodePartitionPath = config.getBoolean(KeyGeneratorOptions.URL_ENCODE_PARTITIONING_OPT_KEY.key(),
        Boolean.parseBoolean(KeyGeneratorOptions.URL_ENCODE_PARTITIONING_OPT_KEY.defaultValue()));
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    Object partitionVal = HoodieAvroUtils.getNestedFieldVal(record, getPartitionPathFields().get(0), true);
    if (partitionVal == null) {
      partitionVal = getDefaultPartitionVal();
    }
    try {
      return getPartitionPath(partitionVal);
    } catch (Exception e) {
      throw new HoodieKeyGeneratorException("Unable to parse input partition field :" + partitionVal, e);
    }
  }

  /**
   * Set default value to partitionVal if the input value of partitionPathField is null.
   */
  public Object getDefaultPartitionVal() {
    long result = 1;
    if (timestampType == TimestampType.DATE_STRING || timestampType == TimestampType.MIXED) {
      // since partitionVal is null, we can set a default value of any format as TIMESTAMP_INPUT_DATE_FORMAT_PROP
      // configured, here we take the first.
      // {Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP} won't be null, it has been checked in the initialization process of
      // inputFormatter
      String delimiter = parser.getConfigInputDateFormatDelimiter();
      String format = config.getString(Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP, "").split(delimiter)[0];
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);

      // if both input and output timeZone are not configured, use GMT.
      if (null != inputDateTimeZone) {
        return ZonedDateTime.ofInstant(Instant.EPOCH.plusMillis(result), inputDateTimeZone).format(formatter);
      } else if (null != outputDateTimeZone) {
        return ZonedDateTime.ofInstant(Instant.EPOCH.plusMillis(result), inputDateTimeZone).format(formatter);
      } else {
        return ZonedDateTime.ofInstant(Instant.EPOCH.plusMillis(result), ZoneId.of("GMT")).format(formatter);
      }
    }
    return result;
  }

  /**
   * The function takes care of lazily initialising dateTimeFormatter variables only once.
   */
  private void initIfNeeded() {
    if (this.inputFormatter == null) {
      this.inputFormatter = parser.getInputFormatter();
    }
    if (this.partitionFormatter == null) {
      this.partitionFormatter = DateTimeFormatter.ofPattern(outputDateFormat);
      if (this.outputDateTimeZone != null) {
        partitionFormatter = partitionFormatter.withZone(outputDateTimeZone);
      }
    }
  }

  /**
   * Parse and fetch partition path based on data type.
   *
   * @param partitionVal partition path object value fetched from record/row
   * @return the parsed partition path based on data type
   */
  public String getPartitionPath(Object partitionVal) {
    initIfNeeded();
    long timeMs;
    if (partitionVal instanceof Double) {
      timeMs = convertLongTimeToMillis(((Double) partitionVal).longValue());
    } else if (partitionVal instanceof Float) {
      timeMs = convertLongTimeToMillis(((Float) partitionVal).longValue());
    } else if (partitionVal instanceof Long) {
      timeMs = convertLongTimeToMillis((Long) partitionVal);
    } else if (partitionVal instanceof Integer) {
      timeMs = convertLongTimeToMillis(((Integer) partitionVal).longValue());
    } else if (partitionVal instanceof BigDecimal) {
      timeMs = convertLongTimeToMillis(((BigDecimal) partitionVal).longValue());
    } else if (partitionVal instanceof CharSequence) {
      if (!inputFormatter.isPresent()) {
        throw new HoodieException("Missing inputformatter. Ensure " + Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP + " config is set when timestampType is DATE_STRING or MIXED!");
      }
      ZonedDateTime parsedDateTime = ZonedDateTime.parse(partitionVal.toString(), inputFormatter.get());
      if (this.outputDateTimeZone == null) {
        // Use the timezone that came off the date that was passed in, if it had one
        partitionFormatter = partitionFormatter.withZone(parsedDateTime.getZone());
      }
      timeMs = parsedDateTime.toInstant().toEpochMilli();
    } else {
      throw new HoodieNotSupportedException(
          "Unexpected type for partition field: " + partitionVal.getClass().getName());
    }
    ZoneId zoneId = outputDateTimeZone == null ? ZoneId.systemDefault() : outputDateTimeZone;
    ZonedDateTime timestamp = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timeMs), zoneId);
    String partitionPath = timestamp.format(partitionFormatter);
    if (encodePartitionPath) {
      partitionPath = PartitionPathEncodeUtils.escapePathName(partitionPath);
    }
    return hiveStylePartitioning ? getPartitionPathFields().get(0) + "=" + partitionPath : partitionPath;
  }

  private long convertLongTimeToMillis(Long partitionVal) {
    if (timeUnit == null) {
      // should not be possible
      throw new RuntimeException(Config.INPUT_TIME_UNIT + " is not specified but scalar it supplied as time value");
    }
    return MILLISECONDS.convert(partitionVal, timeUnit);
  }

}

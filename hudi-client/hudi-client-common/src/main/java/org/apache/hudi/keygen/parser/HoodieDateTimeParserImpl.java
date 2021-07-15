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

package org.apache.hudi.keygen.parser;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator.Config;
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator.TimestampType;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.Collections;
import java.util.TimeZone;

public class HoodieDateTimeParserImpl extends AbstractHoodieDateTimeParser {

  private String configInputDateFormatList;
  private final ZoneId inputDateTimeZone;

  public HoodieDateTimeParserImpl(TypedProperties config) {
    super(config);
    KeyGenUtils.checkRequiredProperties(config, Arrays.asList(Config.TIMESTAMP_TYPE_FIELD_PROP, Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP));
    this.inputDateTimeZone = getInputDateTimeZone();
  }

  private DateTimeFormatter getInputDateFormatter() {
    if (this.configInputDateFormatList.isEmpty()) {
      throw new IllegalArgumentException(Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP + " configuration is required");
    }

    DateTimeFormatterBuilder formatterBuilder = new DateTimeFormatterBuilder();
    Arrays.stream(this.configInputDateFormatList.split(super.configInputDateFormatDelimiter))
            .map(String::trim).map(DateTimeFormatter::ofPattern).forEach(formatterBuilder::appendOptional);
    DateTimeFormatter formatter = formatterBuilder.parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
            .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
            .toFormatter();
    if (this.inputDateTimeZone != null) {
      formatter = formatter.withZone(this.inputDateTimeZone);
    } else {
      formatter = formatter.withZone(ZoneId.systemDefault());
    }
    return formatter;
  }

  @Override
  public String getOutputDateFormat() {
    return config.getString(Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP);
  }

  @Override
  public Option<DateTimeFormatter> getInputFormatter() {
    TimestampType timestampType = TimestampType.valueOf(config.getString(Config.TIMESTAMP_TYPE_FIELD_PROP));
    if (timestampType == TimestampType.DATE_STRING || timestampType == TimestampType.MIXED) {
      KeyGenUtils.checkRequiredProperties(config,
          Collections.singletonList(Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP));
      this.configInputDateFormatList = config.getString(Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP, "");
      return Option.of(getInputDateFormatter());
    }

    return Option.empty();
  }

  @Override
  public ZoneId getInputDateTimeZone() {
    String inputTimeZone;
    if (config.containsKey(Config.TIMESTAMP_TIMEZONE_FORMAT_PROP)) {
      inputTimeZone = config.getString(Config.TIMESTAMP_TIMEZONE_FORMAT_PROP, "GMT");
    } else {
      inputTimeZone = config.getString(Config.TIMESTAMP_INPUT_TIMEZONE_FORMAT_PROP, "");
    }
    return !inputTimeZone.trim().isEmpty() ? TimeZone.getTimeZone(inputTimeZone).toZoneId() : null;
  }

  @Override
  public ZoneId getOutputDateTimeZone() {
    String outputTimeZone;
    if (config.containsKey(Config.TIMESTAMP_TIMEZONE_FORMAT_PROP)) {
      outputTimeZone = config.getString(Config.TIMESTAMP_TIMEZONE_FORMAT_PROP, "GMT");
    } else {
      outputTimeZone = config.getString(Config.TIMESTAMP_OUTPUT_TIMEZONE_FORMAT_PROP, "");
    }
    return !outputTimeZone.trim().isEmpty() ? TimeZone.getTimeZone(outputTimeZone).toZoneId() : null;
  }

}

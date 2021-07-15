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
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator.Config;

import java.io.Serializable;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public abstract class AbstractHoodieDateTimeParser implements Serializable {

  protected final TypedProperties config;
  protected final String configInputDateFormatDelimiter;

  public AbstractHoodieDateTimeParser(TypedProperties config) {
    this.config = config;
    this.configInputDateFormatDelimiter = initInputDateFormatDelimiter();
  }

  private String initInputDateFormatDelimiter() {
    String inputDateFormatDelimiter = config.getString(Config.TIMESTAMP_INPUT_DATE_FORMAT_LIST_DELIMITER_REGEX_PROP, ",").trim();
    return inputDateFormatDelimiter.isEmpty() ? "," : inputDateFormatDelimiter;
  }

  /**
   * Returns the output date format in which the partition paths will be created for the hudi dataset.
   */
  public String getOutputDateFormat() {
    return config.getString(Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP);
  }

  /**
   * Returns input formats in which datetime based values might be coming in incoming records.
   */
  public abstract Option<DateTimeFormatter> getInputFormatter();

  /**
   * Returns the datetime zone one should expect the incoming values into.
   */
  public abstract ZoneId getInputDateTimeZone();

  /**
   * Returns the datetime zone using which the final partition paths for hudi dataset are created.
   */
  public abstract ZoneId getOutputDateTimeZone();

  /**
   * Returns the input date format delimiter, comma by default.
   */
  public String getConfigInputDateFormatDelimiter() {
    return this.configInputDateFormatDelimiter;
  }

}

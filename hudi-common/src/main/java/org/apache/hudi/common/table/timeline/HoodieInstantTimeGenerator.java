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

package org.apache.hudi.common.table.timeline;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Utility class to generate and parse timestamps used in Instants.
 */
public class HoodieInstantTimeGenerator {
  // Format of the timestamp used for an Instant
  private static final String INSTANT_TIMESTAMP_FORMAT = "yyyyMMddHHmmss";
  // Formatter to generate Instant timestamps
  private static DateTimeFormatter INSTANT_TIME_FORMATTER = DateTimeFormatter.ofPattern(INSTANT_TIMESTAMP_FORMAT);
  // The last Instant timestamp generated
  private static AtomicReference<String> lastInstantTime = new AtomicReference<>(String.valueOf(Integer.MIN_VALUE));
  private static final String ALL_ZERO_TIMESTAMP = "00000000000000";

  /**
   * Returns next instant time that adds N milliseconds to the current time.
   * Ensures each instant time is atleast 1 second apart since we create instant times at second granularity
   *
   * @param milliseconds Milliseconds to add to current time while generating the new instant time
   */
  public static String createNewInstantTime(long milliseconds) {
    return lastInstantTime.updateAndGet((oldVal) -> {
      String newCommitTime;
      do {
        Date d = new Date(System.currentTimeMillis() + milliseconds);
        newCommitTime = INSTANT_TIME_FORMATTER.format(convertDateToTemporalAccessor(d));
      } while (HoodieTimeline.compareTimestamps(newCommitTime, HoodieActiveTimeline.LESSER_THAN_OR_EQUALS, oldVal));
      return newCommitTime;
    });
  }

  public static Date parseInstantTime(String timestamp) {
    try {
      LocalDateTime dt = LocalDateTime.parse(timestamp, INSTANT_TIME_FORMATTER);
      return Date.from(dt.atZone(ZoneId.systemDefault()).toInstant());
    } catch (DateTimeParseException e) {
      // Special handling for all zero timestamp which is not parsable by DateTimeFormatter
      if (timestamp.equals(ALL_ZERO_TIMESTAMP)) {
        return new Date(0);
      }

      throw e;
    }
  }

  public static String formatInstantTime(Instant timestamp) {
    return INSTANT_TIME_FORMATTER.format(timestamp);
  }

  public static String formatInstantTime(Date timestamp) {
    return INSTANT_TIME_FORMATTER.format(convertDateToTemporalAccessor(timestamp));
  }

  private static TemporalAccessor convertDateToTemporalAccessor(Date d) {
    return d.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
  }
}

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

package org.apache.hudi.hive;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;

/**
 * HDFS Path contain hive partition values for the keys it is partitioned on. This mapping is not straight forward and
 * requires a pluggable implementation to extract the partition value from HDFS path.
 * <p>
 * This implementation extracts datestr=yyyy-mm-dd from path of type /yyyy/mm/dd
 */
public class SlashEncodedDayPartitionValueExtractor implements PartitionValueExtractor {

  private static final long serialVersionUID = 1L;
  private transient DateTimeFormatter dtfOut;

  public SlashEncodedDayPartitionValueExtractor() {
    this.dtfOut = DateTimeFormatter.ofPattern("yyyy-MM-dd");
  }

  private DateTimeFormatter getDtfOut() {
    if (dtfOut == null) {
      dtfOut = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    }
    return dtfOut;
  }

  @Override
  public List<String> extractPartitionValuesInPath(String partitionPath) {
    // partition path is expected to be in this format yyyy/mm/dd
    String[] splits = partitionPath.split("/");
    if (splits.length != 3) {
      throw new IllegalArgumentException("Partition path " + partitionPath + " is not in the form yyyy/mm/dd ");
    }
    // Get the partition part and remove the / as well at the end
    int year = Integer.parseInt(splits[0].contains("=") ? splits[0].split("=")[1] : splits[0]);
    int mm = Integer.parseInt(splits[1].contains("=") ? splits[1].split("=")[1] : splits[1]);
    int dd = Integer.parseInt(splits[2].contains("=") ? splits[2].split("=")[1] : splits[2]);
    ZonedDateTime dateTime = ZonedDateTime.of(LocalDateTime.of(year, mm, dd, 0, 0), ZoneId.systemDefault());

    return Collections.singletonList(dateTime.format(getDtfOut()));
  }
}

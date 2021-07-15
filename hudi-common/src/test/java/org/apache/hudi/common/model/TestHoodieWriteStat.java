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

package org.apache.hudi.common.model;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests hoodie write stat {@link HoodieWriteStat}.
 */
public class TestHoodieWriteStat {

  @Test
  public void testSetPaths() {
    String instantTime = HoodieActiveTimeline.formatInstantTime(new Date());
    String basePathString = "/data/tables/some-hoodie-table";
    String partitionPathString = "2017/12/31";
    String fileName = UUID.randomUUID().toString();
    String writeToken = "1-0-1";

    Path basePath = new Path(basePathString);
    Path partitionPath = new Path(basePath, partitionPathString);

    Path finalizeFilePath = new Path(partitionPath, FSUtils.makeDataFileName(instantTime, writeToken, fileName));
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPath(basePath, finalizeFilePath);
    assertEquals(finalizeFilePath, new Path(basePath, writeStat.getPath()));

    // test for null tempFilePath
    writeStat = new HoodieWriteStat();
    writeStat.setPath(basePath, finalizeFilePath);
    assertEquals(finalizeFilePath, new Path(basePath, writeStat.getPath()));
    assertNull(writeStat.getTempPath());
  }
}

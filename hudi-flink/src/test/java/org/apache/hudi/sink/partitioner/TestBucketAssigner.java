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

package org.apache.hudi.sink.partitioner;

import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.sink.partitioner.profile.WriteProfile;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.BucketType;
import org.apache.hudi.table.action.commit.SmallFile;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link BucketAssigner}.
 */
public class TestBucketAssigner {
  private HoodieWriteConfig writeConfig;
  private HoodieFlinkEngineContext context;
  private Configuration conf;

  @TempDir
  File tempFile;

  @BeforeEach
  public void before() throws IOException {
    final String basePath = tempFile.getAbsolutePath();
    conf = TestConfigurations.getDefaultConf(basePath);

    writeConfig = StreamerUtil.getHoodieClientConfig(conf);
    context = new HoodieFlinkEngineContext(
        new SerializableConfiguration(StreamerUtil.getHadoopConf()),
        new FlinkTaskContextSupplier(null));
    StreamerUtil.initTableIfNotExists(conf);
  }

  @Test
  public void testAddUpdate() {
    MockBucketAssigner mockBucketAssigner = new MockBucketAssigner(context, writeConfig);
    BucketInfo bucketInfo = mockBucketAssigner.addUpdate("par1", "file_id_0");
    assertBucketEquals(bucketInfo, "par1", BucketType.UPDATE, "file_id_0");

    mockBucketAssigner.addUpdate("par1", "file_id_0");
    bucketInfo = mockBucketAssigner.addUpdate("par1", "file_id_0");
    assertBucketEquals(bucketInfo, "par1", BucketType.UPDATE, "file_id_0");

    mockBucketAssigner.addUpdate("par1", "file_id_1");
    bucketInfo = mockBucketAssigner.addUpdate("par1", "file_id_1");
    assertBucketEquals(bucketInfo, "par1", BucketType.UPDATE, "file_id_1");

    bucketInfo = mockBucketAssigner.addUpdate("par2", "file_id_0");
    assertBucketEquals(bucketInfo, "par2", BucketType.UPDATE, "file_id_0");

    bucketInfo = mockBucketAssigner.addUpdate("par3", "file_id_2");
    assertBucketEquals(bucketInfo, "par3", BucketType.UPDATE, "file_id_2");
  }

  @Test
  public void testAddInsert() {
    MockBucketAssigner mockBucketAssigner = new MockBucketAssigner(context, writeConfig);
    BucketInfo bucketInfo = mockBucketAssigner.addInsert("par1");
    assertBucketEquals(bucketInfo, "par1", BucketType.INSERT);

    mockBucketAssigner.addInsert("par1");
    bucketInfo = mockBucketAssigner.addInsert("par1");
    assertBucketEquals(bucketInfo, "par1", BucketType.INSERT);

    mockBucketAssigner.addInsert("par2");
    bucketInfo = mockBucketAssigner.addInsert("par2");
    assertBucketEquals(bucketInfo, "par2", BucketType.INSERT);

    bucketInfo = mockBucketAssigner.addInsert("par3");
    assertBucketEquals(bucketInfo, "par3", BucketType.INSERT);

    bucketInfo = mockBucketAssigner.addInsert("par3");
    assertBucketEquals(bucketInfo, "par3", BucketType.INSERT);
  }

  @Test
  public void testInsertOverBucketAssigned() {
    conf.setInteger(HoodieCompactionConfig.COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE.key(), 2);
    writeConfig = StreamerUtil.getHoodieClientConfig(conf);

    MockBucketAssigner mockBucketAssigner = new MockBucketAssigner(context, writeConfig);
    BucketInfo bucketInfo1 = mockBucketAssigner.addInsert("par1");
    assertBucketEquals(bucketInfo1, "par1", BucketType.INSERT);

    BucketInfo bucketInfo2 = mockBucketAssigner.addInsert("par1");
    assertBucketEquals(bucketInfo2, "par1", BucketType.INSERT);

    assertEquals(bucketInfo1, bucketInfo2);

    BucketInfo bucketInfo3 = mockBucketAssigner.addInsert("par1");
    assertBucketEquals(bucketInfo3, "par1", BucketType.INSERT);

    assertNotEquals(bucketInfo1, bucketInfo3);
  }

  @Test
  public void testInsertWithSmallFiles() {
    SmallFile f0 = new SmallFile();
    f0.location = new HoodieRecordLocation("t0", "f0");
    f0.sizeBytes = 12;

    SmallFile f1 = new SmallFile();
    f1.location = new HoodieRecordLocation("t0", "f1");
    f1.sizeBytes = 122879; // no left space to append new records to this bucket

    SmallFile f2 = new SmallFile();
    f2.location = new HoodieRecordLocation("t0", "f2");
    f2.sizeBytes = 56;

    Map<String, List<SmallFile>> smallFilesMap = new HashMap<>();
    smallFilesMap.put("par1", Arrays.asList(f0, f1));
    smallFilesMap.put("par2", Collections.singletonList(f2));

    MockBucketAssigner mockBucketAssigner = new MockBucketAssigner(context, writeConfig, smallFilesMap);
    BucketInfo bucketInfo = mockBucketAssigner.addInsert("par1");
    assertBucketEquals(bucketInfo, "par1", BucketType.UPDATE, "f0");

    mockBucketAssigner.addInsert("par1");
    bucketInfo = mockBucketAssigner.addInsert("par1");
    assertBucketEquals(bucketInfo, "par1", BucketType.UPDATE, "f0");

    mockBucketAssigner.addInsert("par2");
    bucketInfo = mockBucketAssigner.addInsert("par2");
    assertBucketEquals(bucketInfo, "par2", BucketType.UPDATE, "f2");

    bucketInfo = mockBucketAssigner.addInsert("par3");
    assertBucketEquals(bucketInfo, "par3", BucketType.INSERT);

    bucketInfo = mockBucketAssigner.addInsert("par3");
    assertBucketEquals(bucketInfo, "par3", BucketType.INSERT);
  }

  /**
   * Test that only partial small files are assigned to the task.
   */
  @Test
  public void testInsertWithPartialSmallFiles() {
    SmallFile f0 = new SmallFile();
    f0.location = new HoodieRecordLocation("t0", "f0");
    f0.sizeBytes = 12;

    SmallFile f1 = new SmallFile();
    f1.location = new HoodieRecordLocation("t0", "f1");
    f1.sizeBytes = 122879; // no left space to append new records to this bucket

    SmallFile f2 = new SmallFile();
    f2.location = new HoodieRecordLocation("t0", "f2");
    f2.sizeBytes = 56;

    Map<String, List<SmallFile>> smallFilesMap = new HashMap<>();
    smallFilesMap.put("par1", Arrays.asList(f0, f1, f2));

    MockBucketAssigner mockBucketAssigner = new MockBucketAssigner(0, 2, context, writeConfig, smallFilesMap);
    BucketInfo bucketInfo = mockBucketAssigner.addInsert("par1");
    assertBucketEquals(bucketInfo, "par1", BucketType.UPDATE, "f0");

    mockBucketAssigner.addInsert("par1");
    bucketInfo = mockBucketAssigner.addInsert("par1");
    assertBucketEquals(bucketInfo, "par1", BucketType.UPDATE, "f0");

    bucketInfo = mockBucketAssigner.addInsert("par3");
    assertBucketEquals(bucketInfo, "par3", BucketType.INSERT);

    bucketInfo = mockBucketAssigner.addInsert("par3");
    assertBucketEquals(bucketInfo, "par3", BucketType.INSERT);

    MockBucketAssigner mockBucketAssigner2 = new MockBucketAssigner(1, 2, context, writeConfig, smallFilesMap);
    BucketInfo bucketInfo2 = mockBucketAssigner2.addInsert("par1");
    assertBucketEquals(bucketInfo2, "par1", BucketType.UPDATE, "f1");

    mockBucketAssigner2.addInsert("par1");
    bucketInfo2 = mockBucketAssigner2.addInsert("par1");
    assertBucketEquals(bucketInfo2, "par1", BucketType.UPDATE, "f1");

    bucketInfo2 = mockBucketAssigner2.addInsert("par3");
    assertBucketEquals(bucketInfo2, "par3", BucketType.INSERT);

    bucketInfo2 = mockBucketAssigner2.addInsert("par3");
    assertBucketEquals(bucketInfo2, "par3", BucketType.INSERT);
  }

  @Test
  public void testUpdateAndInsertWithSmallFiles() {
    SmallFile f0 = new SmallFile();
    f0.location = new HoodieRecordLocation("t0", "f0");
    f0.sizeBytes = 12;

    SmallFile f1 = new SmallFile();
    f1.location = new HoodieRecordLocation("t0", "f1");
    f1.sizeBytes = 122879; // no left space to append new records to this bucket

    SmallFile f2 = new SmallFile();
    f2.location = new HoodieRecordLocation("t0", "f2");
    f2.sizeBytes = 56;

    Map<String, List<SmallFile>> smallFilesMap = new HashMap<>();
    smallFilesMap.put("par1", Arrays.asList(f0, f1));
    smallFilesMap.put("par2", Collections.singletonList(f2));

    MockBucketAssigner mockBucketAssigner = new MockBucketAssigner(context, writeConfig, smallFilesMap);
    mockBucketAssigner.addUpdate("par1", "f0");

    BucketInfo bucketInfo = mockBucketAssigner.addInsert("par1");
    assertBucketEquals(bucketInfo, "par1", BucketType.UPDATE, "f0");

    mockBucketAssigner.addInsert("par1");
    bucketInfo = mockBucketAssigner.addInsert("par1");
    assertBucketEquals(bucketInfo, "par1", BucketType.UPDATE, "f0");

    mockBucketAssigner.addUpdate("par1", "f2");

    mockBucketAssigner.addInsert("par1");
    bucketInfo = mockBucketAssigner.addInsert("par1");
    assertBucketEquals(bucketInfo, "par1", BucketType.UPDATE, "f0");

    mockBucketAssigner.addUpdate("par2", "f0");

    mockBucketAssigner.addInsert("par2");
    bucketInfo = mockBucketAssigner.addInsert("par2");
    assertBucketEquals(bucketInfo, "par2", BucketType.UPDATE, "f2");
  }

  /**
   * Test that only partial small files are assigned to the task.
   */
  @Test
  public void testUpdateAndInsertWithPartialSmallFiles() {
    SmallFile f0 = new SmallFile();
    f0.location = new HoodieRecordLocation("t0", "f0");
    f0.sizeBytes = 12;

    SmallFile f1 = new SmallFile();
    f1.location = new HoodieRecordLocation("t0", "f1");
    f1.sizeBytes = 122879; // no left space to append new records to this bucket

    SmallFile f2 = new SmallFile();
    f2.location = new HoodieRecordLocation("t0", "f2");
    f2.sizeBytes = 56;

    Map<String, List<SmallFile>> smallFilesMap = new HashMap<>();
    smallFilesMap.put("par1", Arrays.asList(f0, f1, f2));

    MockBucketAssigner mockBucketAssigner = new MockBucketAssigner(0, 2, context, writeConfig, smallFilesMap);
    mockBucketAssigner.addUpdate("par1", "f0");

    BucketInfo bucketInfo = mockBucketAssigner.addInsert("par1");
    assertBucketEquals(bucketInfo, "par1", BucketType.UPDATE, "f0");

    mockBucketAssigner.addInsert("par1");
    bucketInfo = mockBucketAssigner.addInsert("par1");
    assertBucketEquals(bucketInfo, "par1", BucketType.UPDATE, "f0");

    mockBucketAssigner.addUpdate("par1", "f2");

    mockBucketAssigner.addInsert("par1");
    bucketInfo = mockBucketAssigner.addInsert("par1");
    assertBucketEquals(bucketInfo, "par1", BucketType.UPDATE, "f0");


    MockBucketAssigner mockBucketAssigner2 = new MockBucketAssigner(1, 2, context, writeConfig, smallFilesMap);
    mockBucketAssigner2.addUpdate("par1", "f0");

    BucketInfo bucketInfo2 = mockBucketAssigner2.addInsert("par1");
    assertBucketEquals(bucketInfo2, "par1", BucketType.UPDATE, "f1");

    mockBucketAssigner2.addInsert("par1");
    bucketInfo2 = mockBucketAssigner2.addInsert("par1");
    assertBucketEquals(bucketInfo2, "par1", BucketType.UPDATE, "f1");

    mockBucketAssigner2.addUpdate("par1", "f2");

    mockBucketAssigner2.addInsert("par1");
    bucketInfo2 = mockBucketAssigner2.addInsert("par1");
    assertBucketEquals(bucketInfo2, "par1", BucketType.UPDATE, "f1");
  }

  @Test
  public void testWriteProfileReload() throws Exception {
    WriteProfile writeProfile = new WriteProfile(writeConfig, context);
    List<SmallFile> smallFiles1 = writeProfile.getSmallFiles("par1");
    assertTrue(smallFiles1.isEmpty(), "Should have no small files");

    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    Option<String> instantOption = getLastCompleteInstant(writeProfile);
    assertFalse(instantOption.isPresent());

    writeProfile.reload(1);
    String instant1 = getLastCompleteInstant(writeProfile).orElse(null);
    assertNotNull(instant1);
    List<SmallFile> smallFiles2 = writeProfile.getSmallFiles("par1");
    assertThat("Should have 1 small file", smallFiles2.size(), is(1));
    assertThat("Small file should have same timestamp as last complete instant",
        smallFiles2.get(0).location.getInstantTime(), is(instant1));

    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    List<SmallFile> smallFiles3 = writeProfile.getSmallFiles("par1");
    assertThat("Should have 1 small file", smallFiles3.size(), is(1));
    assertThat("Non-reloaded write profile has the same base file view as before",
        smallFiles3.get(0).location.getInstantTime(), is(instant1));

    writeProfile.reload(2);
    String instant2 = getLastCompleteInstant(writeProfile).orElse(null);
    assertNotEquals(instant2, instant1, "Should have new complete instant");
    List<SmallFile> smallFiles4 = writeProfile.getSmallFiles("par1");
    assertThat("Should have 1 small file", smallFiles4.size(), is(1));
    assertThat("Small file should have same timestamp as last complete instant",
        smallFiles4.get(0).location.getInstantTime(), is(instant2));
  }

  @Test
  public void testWriteProfileMetadataCache() throws Exception {
    WriteProfile writeProfile = new WriteProfile(writeConfig, context);
    assertTrue(writeProfile.getMetadataCache().isEmpty(), "Empty table should no have any instant metadata");

    HoodieTimeline emptyTimeline = writeProfile.getTable().getActiveTimeline();

    // write 3 instants of data
    for (int i = 0; i < 3; i++) {
      TestData.writeData(TestData.DATA_SET_INSERT, conf);
    }
    writeProfile.reload(1);
    assertThat("Metadata cache should have same number entries as timeline instants",
        writeProfile.getMetadataCache().size(), is(3));

    writeProfile.getSmallFiles("par1");
    assertThat("The metadata should be reused",
        writeProfile.getMetadataCache().size(), is(3));

    writeProfile.reload(2);
    writeProfile.initFSViewIfNecessary(emptyTimeline);
    assertTrue(writeProfile.getMetadataCache().isEmpty(), "Metadata cache should be all cleaned");
  }

  private static Option<String> getLastCompleteInstant(WriteProfile profile) {
    return profile.getTable().getMetaClient().getCommitsTimeline()
        .filterCompletedInstants().lastInstant().map(HoodieInstant::getTimestamp);
  }

  private void assertBucketEquals(
      BucketInfo bucketInfo,
      String partition,
      BucketType bucketType,
      String fileId) {
    BucketInfo actual = new BucketInfo(bucketType, fileId, partition);
    assertThat(bucketInfo, is(actual));
  }

  private void assertBucketEquals(
      BucketInfo bucketInfo,
      String partition,
      BucketType bucketType) {
    assertThat(bucketInfo.getPartitionPath(), is(partition));
    assertThat(bucketInfo.getBucketType(), is(bucketType));
  }

  /**
   * Mock BucketAssigner that can specify small files explicitly.
   */
  static class MockBucketAssigner extends BucketAssigner {

    MockBucketAssigner(
        HoodieFlinkEngineContext context,
        HoodieWriteConfig config) {
      this(context, config, Collections.emptyMap());
    }

    MockBucketAssigner(
        HoodieFlinkEngineContext context,
        HoodieWriteConfig config,
        Map<String, List<SmallFile>> smallFilesMap) {
      this(0, 1, context, config, smallFilesMap);
    }

    MockBucketAssigner(
        int taskID,
        int numTasks,
        HoodieFlinkEngineContext context,
        HoodieWriteConfig config,
        Map<String, List<SmallFile>> smallFilesMap) {
      super(taskID, numTasks, new MockWriteProfile(config, context, smallFilesMap), config);
    }
  }

  /**
   * Mock WriteProfile that can specify small files explicitly.
   */
  static class MockWriteProfile extends WriteProfile {
    private final Map<String, List<SmallFile>> smallFilesMap;

    public MockWriteProfile(HoodieWriteConfig config, HoodieFlinkEngineContext context, Map<String, List<SmallFile>> smallFilesMap) {
      super(config, context);
      this.smallFilesMap = smallFilesMap;
    }

    @Override
    protected List<SmallFile> smallFilesProfile(String partitionPath) {
      if (this.smallFilesMap.containsKey(partitionPath)) {
        return this.smallFilesMap.get(partitionPath);
      }
      return Collections.emptyList();
    }
  }
}

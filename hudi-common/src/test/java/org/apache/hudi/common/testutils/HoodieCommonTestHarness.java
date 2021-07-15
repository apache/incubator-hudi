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

package org.apache.hudi.common.testutils;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.exception.HoodieIOException;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * The common hoodie test harness to provide the basic infrastructure.
 */
public class HoodieCommonTestHarness {

  protected String basePath = null;
  protected transient HoodieTestDataGenerator dataGen = null;
  protected transient HoodieTableMetaClient metaClient;
  public java.nio.file.Path tempDir = Paths.get("/tmp");

  /**
   * Initializes basePath.
   */
  protected void initPath() {
    try {
      java.nio.file.Path basePath = tempDir.resolve("dataset" + UUID.randomUUID().toString());
      java.nio.file.Files.createDirectories(basePath);
      this.basePath = basePath.toString();
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * Initializes a test data generator which used to generate test datas.
   *
   */
  protected void initTestDataGenerator() {
    dataGen = new HoodieTestDataGenerator();
  }

  /**
   * Cleanups test data generator.
   *
   */
  protected void cleanupTestDataGenerator() {
    if (dataGen != null) {
      dataGen = null;
    }
  }

  /**
   * Initializes an instance of {@link HoodieTableMetaClient} with a special table type specified by
   * {@code getTableType()}.
   *
   * @throws IOException
   */
  protected void initMetaClient() throws IOException {
    metaClient = HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), getTableType());
    basePath = metaClient.getBasePath();
  }

  protected void refreshFsView() throws IOException {
    metaClient = HoodieTableMetaClient.builder().setConf(metaClient.getHadoopConf()).setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build();
  }

  protected SyncableFileSystemView getFileSystemView(HoodieTimeline timeline) throws IOException {
    return getFileSystemView(timeline, false);
  }

  protected SyncableFileSystemView getFileSystemView(HoodieTimeline timeline, boolean enableIncrementalTimelineSync) {
    return new HoodieTableFileSystemView(metaClient, timeline, enableIncrementalTimelineSync);
  }

  protected SyncableFileSystemView getFileSystemView(HoodieTableMetaClient metaClient) throws IOException {
    return getFileSystemView(metaClient, metaClient.getActiveTimeline().filterCompletedAndCompactionInstants());
  }

  protected SyncableFileSystemView getFileSystemView(HoodieTableMetaClient metaClient, HoodieTimeline timeline)
      throws IOException {
    return getFileSystemView(timeline, true);
  }

  protected SyncableFileSystemView getFileSystemViewWithUnCommittedSlices(HoodieTableMetaClient metaClient) {
    try {
      return new HoodieTableFileSystemView(metaClient,
              metaClient.getActiveTimeline(),
              HoodieTestTable.of(metaClient).listAllBaseAndLogFiles()
      );
    } catch (IOException ioe) {
      throw new HoodieIOException("Error getting file system view", ioe);
    }
  }

  /**
   * Gets a default {@link HoodieTableType#COPY_ON_WRITE} table type. Sub-classes can override this method to specify a
   * new table type.
   *
   * @return an instance of Hoodie table type.
   */
  protected HoodieTableType getTableType() {
    return HoodieTableType.COPY_ON_WRITE;
  }
}

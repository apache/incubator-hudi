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

package org.apache.hudi.common.table.view;

import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TableFileSystemView Implementations based on in-memory storage along with inflight commit.
 * 
 * @see TableFileSystemView
 * @since 0.3.0
 */
public class HoodieTablePreCommitFileSystemView extends HoodieTableFileSystemView {
  
  private Map<String, List<String>> partitionToReplaceFileIds;
  private String instantTime;

  /**
   * Create a file system view, as of the given timeline.
   */
  public HoodieTablePreCommitFileSystemView(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline,
                                            Map<String, List<String>> partitionToReplaceFileIds, String instantTime) {
    this(metaClient, visibleActiveTimeline, false, partitionToReplaceFileIds, instantTime);
  }

  /**
   * Create a file system view, as of the given timeline.
   */
  public HoodieTablePreCommitFileSystemView(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline,
                                            boolean enableIncrementalTimelineSync,
                                            Map<String, List<String>> partitionToReplaceFileIds, String instantTime) {
    super(metaClient, visibleActiveTimeline, enableIncrementalTimelineSync);
    this.partitionToReplaceFileIds = partitionToReplaceFileIds;
    this.instantTime = instantTime;
  }

  @Override
  protected void resetViewState() {
    super.resetViewState();
    this.partitionToReplaceFileIds = new HashMap<>();
  }

  @Override
  public void close() {
    this.partitionToReplaceFileIds = Collections.emptyMap();
    super.close();
  }

  @Override
  protected Option<HoodieInstant> getReplaceInstant(final HoodieFileGroupId fileGroupId) {
    Option<HoodieInstant> replaceInstantOption = super.getReplaceInstant(fileGroupId);
    if (replaceInstantOption.isPresent()) {
      return replaceInstantOption;
    } 
    List<String> currentInstantReplaceFiles = partitionToReplaceFileIds.getOrDefault(
        fileGroupId.getPartitionPath(), Collections.emptyList());
    if (currentInstantReplaceFiles.contains(fileGroupId.getFileId())) {
      return Option.of(HoodieTimeline.getReplaceCommitInflightInstant(instantTime));
    }
    return Option.empty();
  }
}

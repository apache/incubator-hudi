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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JavaInsertOverwriteTableCommitActionExecutor<T extends HoodieRecordPayload<T>>
    extends JavaInsertOverwriteCommitActionExecutor<T> {

  public JavaInsertOverwriteTableCommitActionExecutor(HoodieEngineContext context,
                                                      HoodieWriteConfig config, HoodieTable table,
                                                      String instantTime, List<HoodieRecord<T>> inputRecords) {
    super(context, config, table, instantTime, inputRecords, WriteOperationType.INSERT_OVERWRITE_TABLE);
  }

  protected List<String> getAllExistingFileIds(String partitionPath) {
    return table.getSliceView().getLatestFileSlices(partitionPath)
        .map(fg -> fg.getFileId()).distinct().collect(Collectors.toList());
  }

  @Override
  protected Map<String, List<String>> getPartitionToReplacedFileIds(HoodieWriteMetadata<List<WriteStatus>> writeResult) {
    Map<String, List<String>> partitionToExistingFileIds = new HashMap<>();
    List<String> partitionPaths = FSUtils.getAllPartitionPaths(context,
        table.getMetaClient().getBasePath(), config.useFileListingMetadata(),
        config.getFileListingMetadataVerify(), config.shouldAssumeDatePartitioning());

    if (partitionPaths != null && partitionPaths.size() > 0) {
      partitionToExistingFileIds = context.mapToPair(partitionPaths,
          partitionPath -> Pair.of(partitionPath, getAllExistingFileIds(partitionPath)), 1);
    }
    return partitionToExistingFileIds;
  }
}

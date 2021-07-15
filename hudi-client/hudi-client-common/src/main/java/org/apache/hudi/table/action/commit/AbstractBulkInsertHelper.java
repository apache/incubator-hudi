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

import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

public abstract class AbstractBulkInsertHelper<T extends HoodieRecordPayload, I, K, O, R> {

  /**
   * Mark instant as inflight, write input records, update index and return result.
   */
  public abstract HoodieWriteMetadata<O> bulkInsert(I inputRecords, String instantTime,
                                                    HoodieTable<T, I, K, O> table, HoodieWriteConfig config,
                                                    BaseCommitActionExecutor<T, I, K, O, R> executor, boolean performDedupe,
                                                    Option<BulkInsertPartitioner<T>> userDefinedBulkInsertPartitioner);

  /**
   * Only write input records. Does not change timeline/index. Return information about new files created.
   */
  public abstract O bulkInsert(I inputRecords, String instantTime,
                               HoodieTable<T, I, K, O> table, HoodieWriteConfig config,
                               boolean performDedupe,
                               Option<BulkInsertPartitioner<T>> userDefinedBulkInsertPartitioner,
                               boolean addMetadataFields,
                               int parallelism,
                               boolean preserveMetadata);
}

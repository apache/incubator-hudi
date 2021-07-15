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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.execution.JavaLazyInsertIterable;
import org.apache.hudi.execution.bulkinsert.JavaBulkInsertInternalPartitionerFactory;
import org.apache.hudi.io.CreateHandleFactory;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.util.ArrayList;
import java.util.List;

/**
 * A java implementation of {@link AbstractBulkInsertHelper}.
 *
 * @param <T>
 */
@SuppressWarnings("checkstyle:LineLength")
public class JavaBulkInsertHelper<T extends HoodieRecordPayload, R> extends AbstractBulkInsertHelper<T, List<HoodieRecord<T>>,
    List<HoodieKey>, List<WriteStatus>, R> {

  private JavaBulkInsertHelper() {
  }

  private static class BulkInsertHelperHolder {
    private static final JavaBulkInsertHelper JAVA_BULK_INSERT_HELPER = new JavaBulkInsertHelper();
  }

  public static JavaBulkInsertHelper newInstance() {
    return BulkInsertHelperHolder.JAVA_BULK_INSERT_HELPER;
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> bulkInsert(final List<HoodieRecord<T>> inputRecords,
                                                           final String instantTime,
                                                           final HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
                                                           final HoodieWriteConfig config,
                                                           final BaseCommitActionExecutor<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>, R> executor,
                                                           final boolean performDedupe,
                                                           final Option<BulkInsertPartitioner<T>> userDefinedBulkInsertPartitioner) {
    HoodieWriteMetadata result = new HoodieWriteMetadata();

    //transition bulk_insert state to inflight
    table.getActiveTimeline().transitionRequestedToInflight(new HoodieInstant(HoodieInstant.State.REQUESTED,
            table.getMetaClient().getCommitActionType(), instantTime), Option.empty(),
        config.shouldAllowMultiWriteOnSameInstant());
    // write new files
    List<WriteStatus> writeStatuses = bulkInsert(inputRecords, instantTime, table, config, performDedupe, userDefinedBulkInsertPartitioner, false, config.getBulkInsertShuffleParallelism(), false);
    //update index
    ((BaseJavaCommitActionExecutor) executor).updateIndexAndCommitIfNeeded(writeStatuses, result);
    return result;
  }

  @Override
  public List<WriteStatus> bulkInsert(List<HoodieRecord<T>> inputRecords,
                                      String instantTime,
                                      HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
                                      HoodieWriteConfig config,
                                      boolean performDedupe,
                                      Option<BulkInsertPartitioner<T>> userDefinedBulkInsertPartitioner,
                                      boolean useWriterSchema,
                                      int parallelism,
                                      boolean preserveHoodieMetadata) {

    // De-dupe/merge if needed
    List<HoodieRecord<T>> dedupedRecords = inputRecords;

    if (performDedupe) {
      dedupedRecords = (List<HoodieRecord<T>>) JavaWriteHelper.newInstance().combineOnCondition(config.shouldCombineBeforeInsert(), inputRecords,
          parallelism, table);
    }

    final List<HoodieRecord<T>> repartitionedRecords;
    BulkInsertPartitioner partitioner = userDefinedBulkInsertPartitioner.isPresent()
        ? userDefinedBulkInsertPartitioner.get()
        : JavaBulkInsertInternalPartitionerFactory.get(config.getBulkInsertSortMode());
    repartitionedRecords = (List<HoodieRecord<T>>) partitioner.repartitionRecords(dedupedRecords, parallelism);

    String idPfx = FSUtils.createNewFileIdPfx();

    List<WriteStatus> writeStatuses = new ArrayList<>();

    new JavaLazyInsertIterable<>(repartitionedRecords.iterator(), true, config, instantTime, table, idPfx,
        table.getTaskContextSupplier(), new CreateHandleFactory<>()).forEachRemaining(writeStatuses::addAll);

    return writeStatuses;
  }
}

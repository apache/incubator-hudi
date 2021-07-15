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

package org.apache.hudi.io;

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * A HoodieCreateHandle which writes all data into a single file.
 * 
 * Please use this with caution. This can end up creating very large files if not used correctly.
 */
public class HoodieUnboundedCreateHandle<T extends HoodieRecordPayload, I, K, O> extends HoodieCreateHandle<T, I, K, O> {

  private static final Logger LOG = LogManager.getLogger(HoodieUnboundedCreateHandle.class);

  public HoodieUnboundedCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                     String partitionPath, String fileId, TaskContextSupplier taskContextSupplier,
                                     boolean preserveHoodieMetadata) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, Option.empty(),
        taskContextSupplier, preserveHoodieMetadata);
  }

  @Override
  public boolean canWrite(HoodieRecord record) {
    return true;
  }
}

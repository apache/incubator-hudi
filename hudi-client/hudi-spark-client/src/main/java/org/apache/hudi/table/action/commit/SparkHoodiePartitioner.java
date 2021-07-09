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
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.bucket.SparkHiveBucketIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.spark.Partitioner;

public abstract class SparkHoodiePartitioner<T extends HoodieRecordPayload<T>> extends Partitioner {

  /**
   * Stat for the current workload. Helps in determining inserts, upserts etc.
   */
  protected WorkloadProfile profile;

  protected final HoodieTable table;

  public SparkHoodiePartitioner(WorkloadProfile profile,
      HoodieTable table) {
    this.profile = profile;
    this.table = table;
  }

  public abstract BucketInfo getBucketInfo(int bucketNumber);

  protected void checkIndexCompatibility() {
    if (table.getIndex() instanceof SparkHiveBucketIndex) {
      throw new HoodieIndexException("Partitioner " + this.getClass().getSimpleName() + " is not compatible with bucket index");
    }
  }

}

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
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.UpdatePrecombineAvroPayload;
import org.apache.hudi.index.HoodieIndex;

import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

/**
 * A spark implementation of {@link AbstractWriteHelper}.
 *
 * @param <T>
 */
public class SparkWriteHelper<T extends HoodieRecordPayload, R> extends AbstractWriteHelper<T, JavaRDD<HoodieRecord<T>>,
        JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, R> {
  private SparkWriteHelper() {
  }

  private static class WriteHelperHolder {
    private static final SparkWriteHelper SPARK_WRITE_HELPER = new SparkWriteHelper();
  }

  public static SparkWriteHelper newInstance() {
    return WriteHelperHolder.SPARK_WRITE_HELPER;
  }

  @Override
  public JavaRDD<HoodieRecord<T>> deduplicateRecords(JavaRDD<HoodieRecord<T>> records,
                                                     HoodieIndex<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> index,
                                                     int parallelism, SerializableSchema schema) {
    boolean isIndexingGlobal = index.isGlobal();
    return records.mapToPair(record -> {
      HoodieKey hoodieKey = record.getKey();
      // If index used is global, then records are expected to differ in their partitionPath
      Object key = isIndexingGlobal ? hoodieKey.getRecordKey() : hoodieKey;
      return new Tuple2<>(key, record);
    }).reduceByKey((rec1, rec2) -> {
      @SuppressWarnings("unchecked")
      T reducedData;
      //To prevent every records from parsing schema
      if (rec2.getData() instanceof UpdatePrecombineAvroPayload) {
        reducedData = schema.getSchema()!=null ? (T) rec1.getData().preCombine(rec2.getData(), schema.getSchema())
                : (T) rec1.getData().preCombine(rec2.getData());
      } else {
        reducedData = (T) rec1.getData().preCombine(rec2.getData());
      }
      // we cannot allow the user to change the key or partitionPath, since that will affect
      // everything
      // so pick it from one of the records.
      return new HoodieRecord<T>(rec1.getKey(), reducedData);
    }, parallelism).map(Tuple2::_2);
  }

}

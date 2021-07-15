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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.ColumnNotFoundException;
import org.apache.hudi.exception.UpdateKeyNotFoundException;
import org.apache.hudi.exception.WriteOperationException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * subclass of OverwriteWithLatestAvroPayload used for delta streamer.
 *
 * <ol>
 * <li> combineAndGetUpdateValue - Accepts the column names to be updated;
 * <li> splitKeys - Split keys based upon keys;
 * </ol>
 */
public class OverwriteWithCustomAvroPayload extends OverwriteWithLatestAvroPayload {

  public OverwriteWithCustomAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  /**
   * split keys over.
   */
  public List<String> splitKeys(String keys) throws UpdateKeyNotFoundException {
    if (keys == null) {
      throw new UpdateKeyNotFoundException("keys cannot be null");
    } else if (keys.equals("")) {
      throw new UpdateKeyNotFoundException("keys cannot be blank");
    } else {
      return Arrays.stream(keys.split(",")).collect(Collectors.toList());
    }
  }

  /**
   * check column exi.
   */
  public boolean checkColumnExists(List<String> keys, Schema schema) {
    List<Schema.Field> field = schema.getFields();
    List<Schema.Field> common = new ArrayList<>();
    for (Schema.Field columns : field) {
      if (keys.contains(columns.name())) {
        common.add(columns);
      }
    }
    return common.size() == keys.size();
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties properties)
      throws WriteOperationException, IOException, ColumnNotFoundException, UpdateKeyNotFoundException {

    if (!properties.getProperty("hoodie.datasource.write.operation").equals("upsert")) {
      throw new WriteOperationException("write should be upsert");
    }

    Option<IndexedRecord> recordOption = getInsertValue(schema);

    if (!recordOption.isPresent()) {
      return Option.empty();
    }

    GenericRecord existingRecord = (GenericRecord) currentValue;
    GenericRecord incomingRecord = (GenericRecord) recordOption.get();
    List<String> keys = splitKeys(properties.getProperty("hoodie.update.keys"));

    if (checkColumnExists(keys, schema)) {
      for (String key : keys) {
        Object value = incomingRecord.get(key);
        existingRecord.put(key, value);
      }
      return Option.of(existingRecord);
    } else {
      throw new ColumnNotFoundException("Update key not present please check the names");
    }
  }

}

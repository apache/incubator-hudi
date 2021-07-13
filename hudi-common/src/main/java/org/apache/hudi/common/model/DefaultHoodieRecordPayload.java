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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.avro.HoodieAvroUtils.bytesToAvro;
import static org.apache.hudi.avro.HoodieAvroUtils.getNestedFieldVal;

/**
 * {@link HoodieRecordPayload} impl that honors ordering field in both preCombine and combineAndGetUpdateValue.
 * <p>
 * 1. preCombine - Picks the latest delta record for a key, based on an ordering field 2. combineAndGetUpdateValue/getInsertValue - Chooses the latest record based on ordering field value.
 */
public class DefaultHoodieRecordPayload extends OverwriteWithLatestAvroPayload {

  public static final String METADATA_EVENT_TIME_KEY = "metadata.event_time.key";
  private Option<Object> eventTime = Option.empty();

  public DefaultHoodieRecordPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public DefaultHoodieRecordPayload(Option<GenericRecord> record) {
    this(record.isPresent() ? record.get() : null, 0); // natural order
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties properties) throws IOException {
    if (recordBytes.length == 0) {
      return Option.empty();
    }
    HoodieConfig hoodieConfig = new HoodieConfig(properties);
    GenericRecord incomingRecord = bytesToAvro(recordBytes, schema);

    // Null check is needed here to support schema evolution. The record in storage may be from old schema where
    // the new ordering column might not be present and hence returns null.
    if (!needUpdatingPersistedRecord(currentValue, incomingRecord, properties)) {
      return Option.of(currentValue);
    }

    /*
     * We reached a point where the value is disk is older than the incoming record.
     */
    eventTime = Option.ofNullable(getNestedFieldVal(incomingRecord, hoodieConfig
        .getString(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY), true));

    /*
     * Now check if the incoming record is a delete record.
     */
    if (isDeleteRecord(incomingRecord)) {
      return Option.empty();
    } else {
      return Option.of(incomingRecord);
    }
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    Map<String, String> metadata = new HashMap<>();
    if (eventTime.isPresent()) {
      metadata.put(METADATA_EVENT_TIME_KEY, String.valueOf(eventTime.get()));
    }
    return metadata.isEmpty() ? Option.empty() : Option.of(metadata);
  }

  protected boolean needUpdatingPersistedRecord(IndexedRecord currentValue,
                                                IndexedRecord incomingRecord, Properties properties) {
    /*
     * Combining strategy here returns currentValue on disk if incoming record is older.
     * The incoming record can be either a delete (sent as an upsert with _hoodie_is_deleted set to true)
     * or an insert/update record. In any case, if it is older than the record in disk, the currentValue
     * in disk is returned (to be rewritten with new commit time).
     *
     * NOTE: Deletes sent via EmptyHoodieRecordPayload and/or Delete operation type do not hit this code path
     * and need to be dealt with separately.
     */
    Object persistedOrderingVal = getOrderingVal((GenericRecord) currentValue, properties);
    Comparable incomingOrderingVal = (Comparable) getOrderingVal((GenericRecord) incomingRecord, properties);
    return persistedOrderingVal == null || ((Comparable) persistedOrderingVal).compareTo(incomingOrderingVal) <= 0;
  }
}

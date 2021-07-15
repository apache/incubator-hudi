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

import org.apache.avro.Schema;

import java.util.Properties;

/**
 * Holds payload properties that implementation of {@link HoodieRecordPayload} can leverage.
 * Since both payload classes and HoodiePayloadConfig needs to access these props, storing it here in hudi-common.
 */
public class HoodiePayloadProps {

  /**
   * Property for payload ordering field; to be used to merge incoming record with that in storage.
   * Implementations of {@link HoodieRecordPayload} can leverage if required.
   *
   * @see DefaultHoodieRecordPayload
   */
  public static final String PAYLOAD_ORDERING_FIELD_PROP_KEY = "hoodie.payload.ordering.field";

  /**
   * Property for payload event time field; to be used to extract source event time info.
   *
   * @see DefaultHoodieRecordPayload
   */
  public static final String PAYLOAD_EVENT_TIME_FIELD_PROP_KEY = "hoodie.payload.event.time.field";

  /**
   * A runtime config pass to the {@link HoodieRecordPayload#getInsertValue(Schema, Properties)}
   * to tell if the current record is a update record or insert record for mor table.
   */
  public static final String PAYLOAD_IS_UPDATE_RECORD_FOR_MOR = "hoodie.is.update.record.for.mor";
}

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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Unit tests {@link TestOverwriteNonDefaultsWithLatestAvroPayload}.
 */
public class TestOverwriteNonDefaultsWithLatestAvroPayload {
  private Schema schema;

  @BeforeEach
  public void setUp() throws Exception {
    schema = Schema.createRecord(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.STRING), "", null),
            new Schema.Field("partition", Schema.create(Schema.Type.STRING), "", ""),
            new Schema.Field("ts", Schema.create(Schema.Type.LONG), "", null),
            new Schema.Field("_hoodie_is_deleted", Schema.create(Schema.Type.BOOLEAN), "", false),
            new Schema.Field("city", Schema.create(Schema.Type.STRING), "", "NY"),
            new Schema.Field("child", Schema.createArray(Schema.create(Schema.Type.STRING)), "", Collections.emptyList())
            ));
  }

  @Test
  public void testActiveRecords() throws IOException {
    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("id", "1");
    record1.put("partition", "partition1");
    record1.put("ts", 0L);
    record1.put("_hoodie_is_deleted", false);
    record1.put("city", "NY0");
    record1.put("child", Arrays.asList("A"));

    GenericRecord record2 = new GenericData.Record(schema);
    record2.put("id", "2");
    record2.put("partition", "");
    record2.put("ts", 1L);
    record2.put("_hoodie_is_deleted", false);
    record2.put("city", "NY");
    record2.put("child", Collections.emptyList());

    GenericRecord record3 = new GenericData.Record(schema);
    record3.put("id", "2");
    record3.put("partition", "partition1");
    record3.put("ts", 1L);
    record3.put("_hoodie_is_deleted", false);
    record3.put("city", "NY0");
    record3.put("child", Arrays.asList("A"));


    OverwriteNonDefaultsWithLatestAvroPayload payload1 = new OverwriteNonDefaultsWithLatestAvroPayload(record1, 1);
    OverwriteNonDefaultsWithLatestAvroPayload payload2 = new OverwriteNonDefaultsWithLatestAvroPayload(record2, 2);
    assertEquals(payload1.preCombine(payload2), payload2);
    assertEquals(payload2.preCombine(payload1), payload2);

    assertEquals(record1, payload1.getInsertValue(schema).get());
    assertEquals(record2, payload2.getInsertValue(schema).get());

    assertEquals(payload1.combineAndGetUpdateValue(record2, schema).get(), record1);
    assertEquals(payload2.combineAndGetUpdateValue(record1, schema).get(), record3);
  }

  @Test
  public void testDeletedRecord() throws IOException {
    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("id", "1");
    record1.put("partition", "partition0");
    record1.put("ts", 0L);
    record1.put("_hoodie_is_deleted", false);
    record1.put("city", "NY0");
    record1.put("child", Collections.emptyList());

    GenericRecord delRecord1 = new GenericData.Record(schema);
    delRecord1.put("id", "2");
    delRecord1.put("partition", "partition1");
    delRecord1.put("ts", 1L);
    delRecord1.put("_hoodie_is_deleted", true);
    delRecord1.put("city", "NY0");
    delRecord1.put("child", Collections.emptyList());

    GenericRecord record2 = new GenericData.Record(schema);
    record2.put("id", "1");
    record2.put("partition", "partition0");
    record2.put("ts", 0L);
    record2.put("_hoodie_is_deleted", true);
    record2.put("city", "NY0");
    record2.put("child", Collections.emptyList());

    OverwriteNonDefaultsWithLatestAvroPayload payload1 = new OverwriteNonDefaultsWithLatestAvroPayload(record1, 1);
    OverwriteNonDefaultsWithLatestAvroPayload payload2 = new OverwriteNonDefaultsWithLatestAvroPayload(delRecord1, 2);

    assertEquals(payload1.preCombine(payload2), payload2);
    assertEquals(payload2.preCombine(payload1), payload2);

    assertEquals(record1, payload1.getInsertValue(schema).get());
    assertFalse(payload2.getInsertValue(schema).isPresent());

    assertEquals(payload1.combineAndGetUpdateValue(delRecord1, schema).get(), record2);
    assertFalse(payload2.combineAndGetUpdateValue(record1, schema).isPresent());
  }
}

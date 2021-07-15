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

package org.apache.hudi.avro;

import org.apache.avro.JsonProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.exception.SchemaCompatibilityException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests hoodie avro utilities.
 */
public class TestHoodieAvroUtils {

  private static String EVOLVED_SCHEMA = "{\"type\": \"record\",\"name\": \"testrec1\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"},"
      + "{\"name\": \"new_col_not_nullable_default_dummy_val\", \"type\": \"string\", \"default\": \"dummy_val\"},"
      + "{\"name\": \"new_col_nullable_wo_default\", \"type\": [\"int\", \"null\"]},"
      + "{\"name\": \"new_col_nullable_default_null\", \"type\": [\"null\" ,\"string\"],\"default\": null},"
      + "{\"name\": \"new_col_nullable_default_dummy_val\", \"type\": [\"string\" ,\"null\"],\"default\": \"dummy_val\"}]}";


  private static String EXAMPLE_SCHEMA = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"}]}";

  private static int NUM_FIELDS_IN_EXAMPLE_SCHEMA = 4;

  private static String SCHEMA_WITH_METADATA_FIELD = "{\"type\": \"record\",\"name\": \"testrec2\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"},"
      + "{\"name\": \"_hoodie_commit_time\", \"type\": [\"null\", \"string\"]},"
      + "{\"name\": \"nullable_field\",\"type\": [\"null\" ,\"string\"],\"default\": null},"
      + "{\"name\": \"nullable_field_wo_default\",\"type\": [\"null\" ,\"string\"]}]}";

  private static String SCHEMA_WITH_NON_NULLABLE_FIELD = "{\"type\": \"record\",\"name\": \"testrec3\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"},"
      + "{\"name\": \"nullable_field\",\"type\": [\"null\" ,\"string\"],\"default\": null},"
      + "{\"name\": \"non_nullable_field_wo_default\",\"type\": \"string\"},"
      + "{\"name\": \"non_nullable_field_with_default\",\"type\": \"string\", \"default\": \"dummy\"}]}";

  private static String SCHEMA_WITH_NON_NULLABLE_FIELD_WITH_DEFAULT = "{\"type\": \"record\",\"name\": \"testrec4\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"},"
      + "{\"name\": \"nullable_field\",\"type\": [\"null\" ,\"string\"],\"default\": null},"
      + "{\"name\": \"non_nullable_field_with_default\",\"type\": \"string\", \"default\": \"dummy\"}]}";

  @Test
  public void testPropsPresent() {
    Schema schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    boolean piiPresent = false;
    for (Schema.Field field : schema.getFields()) {
      if (HoodieAvroUtils.isMetadataField(field.name())) {
        continue;
      }

      assertNotNull(field.name(), "field name is null");
      Map<String, Object> props = field.getObjectProps();
      assertNotNull(props, "The property is null");

      if (field.name().equals("pii_col")) {
        piiPresent = true;
        assertTrue(props.containsKey("column_category"), "sensitivity_level is removed in field 'pii_col'");
      } else {
        assertEquals(0, props.size(), "The property shows up but not set");
      }
    }
    assertTrue(piiPresent, "column pii_col doesn't show up");
  }

  @Test
  public void testDefaultValue() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EVOLVED_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");
    rec.put("timestamp", 3.5);
    Schema schemaWithMetadata = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(EVOLVED_SCHEMA));
    GenericRecord rec1 = HoodieAvroUtils.rewriteRecord(rec, schemaWithMetadata);
    assertEquals(rec1.get("new_col_not_nullable_default_dummy_val"), "dummy_val");
    assertNull(rec1.get("new_col_nullable_wo_default"));
    assertNull(rec1.get("new_col_nullable_default_null"));
    assertEquals(rec1.get("new_col_nullable_default_dummy_val"), "dummy_val");
    assertNull(rec1.get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
  }

  @Test
  public void testDefaultValueWithSchemaEvolution() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");
    rec.put("timestamp", 3.5);
    GenericRecord rec1 = HoodieAvroUtils.rewriteRecord(rec, new Schema.Parser().parse(EVOLVED_SCHEMA));
    assertEquals(rec1.get("new_col_not_nullable_default_dummy_val"), "dummy_val");
    assertNull(rec1.get("new_col_nullable_wo_default"));
  }

  @Test
  public void testMetadataField() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");
    rec.put("timestamp", 3.5);
    GenericRecord rec1 = HoodieAvroUtils.rewriteRecord(rec, new Schema.Parser().parse(SCHEMA_WITH_METADATA_FIELD));
    assertNull(rec1.get("_hoodie_commit_time"));
    assertNull(rec1.get("nullable_field"));
    assertNull(rec1.get("nullable_field_wo_default"));
  }

  @Test
  public void testNonNullableFieldWithoutDefault() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");
    rec.put("timestamp", 3.5);
    assertThrows(SchemaCompatibilityException.class, () -> HoodieAvroUtils.rewriteRecord(rec, new Schema.Parser().parse(SCHEMA_WITH_NON_NULLABLE_FIELD)));
  }

  @Test
  public void testNonNullableFieldWithDefault() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");
    rec.put("timestamp", 3.5);
    GenericRecord rec1 = HoodieAvroUtils.rewriteRecord(rec, new Schema.Parser().parse(SCHEMA_WITH_NON_NULLABLE_FIELD_WITH_DEFAULT));
    assertEquals(rec1.get("non_nullable_field_with_default"), "dummy");
  }

  @Test
  public void testJsonNodeNullWithDefaultValues() {
    List<Schema.Field> fields = new ArrayList<>();
    Schema initialSchema = Schema.createRecord("test_record", "test record", "org.test.namespace", false);
    Schema.Field field1 = new Schema.Field("key", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field field2 = new Schema.Field("key1", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field field3 = new Schema.Field("key2", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    fields.add(field1);
    fields.add(field2);
    fields.add(field3);
    initialSchema.setFields(fields);
    GenericRecord rec = new GenericData.Record(initialSchema);
    rec.put("key", "val");
    rec.put("key1", "val1");
    rec.put("key2", "val2");

    List<Schema.Field> evolvedFields = new ArrayList<>();
    Schema evolvedSchema = Schema.createRecord("evolved_record", "evolved record", "org.evolved.namespace", false);
    Schema.Field evolvedField1 = new Schema.Field("key", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field evolvedField2 = new Schema.Field("key1", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field evolvedField3 = new Schema.Field("key2", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field evolvedField4 = new Schema.Field("evolved_field", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field evolvedField5 = new Schema.Field("evolved_field1", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    evolvedFields.add(evolvedField1);
    evolvedFields.add(evolvedField2);
    evolvedFields.add(evolvedField3);
    evolvedFields.add(evolvedField4);
    evolvedFields.add(evolvedField5);
    evolvedSchema.setFields(evolvedFields);

    GenericRecord rec1 = HoodieAvroUtils.rewriteRecord(rec, evolvedSchema);
    //evolvedField4.defaultVal() returns a JsonProperties.Null instance.
    assertNull(rec1.get("evolved_field"));
    //evolvedField5.defaultVal() returns null.
    assertNull(rec1.get("evolved_field1"));
  }

  @Test
  public void testAddingAndRemovingMetadataFields() {
    Schema schemaWithMetaCols = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    assertEquals(schemaWithMetaCols.getFields().size(), NUM_FIELDS_IN_EXAMPLE_SCHEMA + HoodieRecord.HOODIE_META_COLUMNS.size());
    Schema schemaWithoutMetaCols = HoodieAvroUtils.removeMetadataFields(schemaWithMetaCols);
    assertEquals(schemaWithoutMetaCols.getFields().size(), NUM_FIELDS_IN_EXAMPLE_SCHEMA);
  }

  @Test
  public void testGetNestedFieldVal() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");

    Object rowKey = HoodieAvroUtils.getNestedFieldVal(rec, "_row_key", true);
    assertEquals(rowKey, "key1");

    Object rowKeyNotExist = HoodieAvroUtils.getNestedFieldVal(rec, "fake_key", true);
    assertNull(rowKeyNotExist);

    // Field does not exist
    try {
      HoodieAvroUtils.getNestedFieldVal(rec, "fake_key", false);
    } catch (Exception e) {
      assertEquals("fake_key(Part -fake_key) field not found in record. Acceptable fields were :[timestamp, _row_key, non_pii_col, pii_col]",
          e.getMessage());
    }

    // Field exist while value not
    try {
      HoodieAvroUtils.getNestedFieldVal(rec, "timestamp", false);
    } catch (Exception e) {
      assertEquals("The value of timestamp can not be null", e.getMessage());
    }
  }

}

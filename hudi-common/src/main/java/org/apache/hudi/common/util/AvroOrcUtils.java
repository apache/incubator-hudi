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

package org.apache.hudi.common.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import java.nio.charset.StandardCharsets;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.StringType;
import org.apache.avro.util.Utf8;
import org.apache.orc.storage.common.type.HiveDecimal;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.ListColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.MapColumnVector;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.storage.ql.exec.vector.UnionColumnVector;
import org.apache.orc.storage.serde2.io.DateWritable;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.orc.TypeDescription;

import static org.apache.avro.JsonProperties.NULL_VALUE;

/**
 * Methods including addToVector, addUnionValue, createOrcSchema are originally from
 * https://github.com/streamsets/datacollector.
 * Source classes:
 * - com.streamsets.pipeline.lib.util.avroorc.AvroToOrcRecordConverter
 * - com.streamsets.pipeline.lib.util.avroorc.AvroToOrcSchemaConverter
 *
 * Changes made:
 * 1. Flatten nullable Avro schema type when the value is not null in `addToVector`.
 * 2. Use getLogicalType(), constants from LogicalTypes instead of getJsonProp() to handle Avro logical types.
 */
public class AvroOrcUtils {

  private static final int MICROS_PER_MILLI = 1000;
  private static final int NANOS_PER_MICRO = 1000;

  /**
   * Add an object (of a given ORC type) to the column vector at a given position.
   *
   * @param type        ORC schema of the value Object.
   * @param colVector   The column vector to store the value Object.
   * @param avroSchema  Avro schema of the value Object.
   *                    Only used to check logical types for timestamp unit conversion.
   * @param value       Object to be added to the column vector
   * @param vectorPos   The position in the vector where value will be stored at.
   */
  public static void addToVector(TypeDescription type, ColumnVector colVector, Schema avroSchema, Object value, int vectorPos) {

    final int currentVecLength = colVector.isNull.length;
    if (vectorPos >= currentVecLength) {
      colVector.ensureSize(2 * currentVecLength, true);
    }
    if (value == null) {
      colVector.isNull[vectorPos] = true;
      colVector.noNulls = false;
      return;
    }

    if (avroSchema.getType().equals(Schema.Type.UNION)) {
      avroSchema = getActualSchemaType(avroSchema);
    }

    LogicalType logicalType = avroSchema != null ? avroSchema.getLogicalType() : null;

    switch (type.getCategory()) {
      case BOOLEAN:
        LongColumnVector boolVec = (LongColumnVector) colVector;
        boolVec.vector[vectorPos] = (boolean) value ? 1 : 0;
        break;
      case BYTE:
        LongColumnVector byteColVec = (LongColumnVector) colVector;
        byteColVec.vector[vectorPos] = (byte) value;
        break;
      case SHORT:
        LongColumnVector shortColVec = (LongColumnVector) colVector;
        shortColVec.vector[vectorPos] = (short) value;
        break;
      case INT:
        // the Avro logical type could be AvroTypeUtil.LOGICAL_TYPE_TIME_MILLIS, but we will ignore that fact here
        // since Orc has no way to represent a time in the way Avro defines it; we will simply preserve the int value
        LongColumnVector intColVec = (LongColumnVector) colVector;
        intColVec.vector[vectorPos] = (int) value;
        break;
      case LONG:
        // the Avro logical type could be AvroTypeUtil.LOGICAL_TYPE_TIME_MICROS, but we will ignore that fact here
        // since Orc has no way to represent a time in the way Avro defines it; we will simply preserve the long value
        LongColumnVector longColVec = (LongColumnVector) colVector;
        longColVec.vector[vectorPos] = (long) value;
        break;
      case FLOAT:
        DoubleColumnVector floatColVec = (DoubleColumnVector) colVector;
        floatColVec.vector[vectorPos] = (float) value;
        break;
      case DOUBLE:
        DoubleColumnVector doubleColVec = (DoubleColumnVector) colVector;
        doubleColVec.vector[vectorPos] = (double) value;
        break;
      case VARCHAR:
      case CHAR:
      case STRING:
        BytesColumnVector bytesColVec = (BytesColumnVector) colVector;
        byte[] bytes = null;

        if (value instanceof String) {
          bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
        } else if (value instanceof Utf8) {
          final Utf8 utf8 = (Utf8) value;
          bytes = utf8.getBytes();
        } else if (value instanceof GenericData.EnumSymbol) {
          bytes = ((GenericData.EnumSymbol) value).toString().getBytes(StandardCharsets.UTF_8);
        } else {
          throw new IllegalStateException(String.format(
              "Unrecognized type for Avro %s field value, which has type %s, value %s",
              type.getCategory().getName(),
              value.getClass().getName(),
              value.toString()
          ));
        }

        if (bytes == null) {
          bytesColVec.isNull[vectorPos] = true;
          bytesColVec.noNulls = false;
        } else {
          bytesColVec.setRef(vectorPos, bytes, 0, bytes.length);
        }
        break;
      case DATE:
        LongColumnVector dateColVec = (LongColumnVector) colVector;
        int daysSinceEpoch;
        if (logicalType instanceof LogicalTypes.Date) {
          daysSinceEpoch = (int) value;
        } else if (value instanceof java.sql.Date) {
          daysSinceEpoch = DateWritable.dateToDays((java.sql.Date) value);
        } else if (value instanceof Date) {
          daysSinceEpoch = DateWritable.millisToDays(((Date) value).getTime());
        } else {
          throw new IllegalStateException(String.format(
              "Unrecognized type for Avro DATE field value, which has type %s, value %s",
              value.getClass().getName(),
              value.toString()
          ));
        }
        dateColVec.vector[vectorPos] = daysSinceEpoch;
        break;
      case TIMESTAMP:
        TimestampColumnVector tsColVec = (TimestampColumnVector) colVector;

        long time;
        int nanos = 0;

        // The unit for Timestamp in ORC is millis, convert timestamp to millis if needed
        if (logicalType instanceof LogicalTypes.TimestampMillis) {
          time = (long) value;
        } else if (logicalType instanceof LogicalTypes.TimestampMicros) {
          final long logicalTsValue = (long) value;
          time = logicalTsValue / MICROS_PER_MILLI;
          nanos = NANOS_PER_MICRO * ((int) (logicalTsValue % MICROS_PER_MILLI));
        } else if (value instanceof Timestamp) {
          Timestamp tsValue = (Timestamp) value;
          time = tsValue.getTime();
          nanos = tsValue.getNanos();
        } else if (value instanceof java.sql.Date) {
          java.sql.Date sqlDateValue = (java.sql.Date) value;
          time = sqlDateValue.getTime();
        } else if (value instanceof Date) {
          Date dateValue = (Date) value;
          time = dateValue.getTime();
        } else {
          throw new IllegalStateException(String.format(
              "Unrecognized type for Avro TIMESTAMP field value, which has type %s, value %s",
              value.getClass().getName(),
              value.toString()
          ));
        }

        tsColVec.time[vectorPos] = time;
        tsColVec.nanos[vectorPos] = nanos;
        break;
      case BINARY:
        BytesColumnVector binaryColVec = (BytesColumnVector) colVector;

        byte[] binaryBytes;
        if (value instanceof GenericData.Fixed) {
          binaryBytes = ((GenericData.Fixed)value).bytes();
        } else if (value instanceof ByteBuffer) {
          final ByteBuffer byteBuffer = (ByteBuffer) value;
          binaryBytes = new byte[byteBuffer.remaining()];
          byteBuffer.get(binaryBytes);
        } else if (value instanceof byte[]) {
          binaryBytes = (byte[]) value;
        } else {
          throw new IllegalStateException(String.format(
              "Unrecognized type for Avro BINARY field value, which has type %s, value %s",
              value.getClass().getName(),
              value.toString()
          ));
        }
        binaryColVec.setRef(vectorPos, binaryBytes, 0, binaryBytes.length);
        break;
      case DECIMAL:
        DecimalColumnVector decimalColVec = (DecimalColumnVector) colVector;
        HiveDecimal decimalValue;
        if (value instanceof BigDecimal) {
          final BigDecimal decimal = (BigDecimal) value;
          decimalValue = HiveDecimal.create(decimal);
        } else if (value instanceof ByteBuffer) {
          final ByteBuffer byteBuffer = (ByteBuffer) value;
          final byte[] decimalBytes = new byte[byteBuffer.remaining()];
          byteBuffer.get(decimalBytes);
          final BigInteger bigInt = new BigInteger(decimalBytes);
          final int scale = type.getScale();
          BigDecimal bigDecVal = new BigDecimal(bigInt, scale);

          decimalValue = HiveDecimal.create(bigDecVal);
          if (decimalValue == null && decimalBytes.length > 0) {
            throw new IllegalStateException(
                "Unexpected read null HiveDecimal from bytes (base-64 encoded): "
                    + Base64.getEncoder().encodeToString(decimalBytes)
            );
          }
        } else if (value instanceof GenericData.Fixed) {
          final BigDecimal decimal = new Conversions.DecimalConversion()
              .fromFixed((GenericData.Fixed) value, avroSchema, logicalType);
          decimalValue = HiveDecimal.create(decimal);
        } else {
          throw new IllegalStateException(String.format(
              "Unexpected type for decimal (%s), cannot convert from Avro value",
              value.getClass().getCanonicalName()
          ));
        }
        if (decimalValue == null) {
          decimalColVec.isNull[vectorPos] = true;
          decimalColVec.noNulls = false;
        } else {
          decimalColVec.set(vectorPos, decimalValue);
        }
        break;
      case LIST:
        List<?> list = (List<?>) value;
        ListColumnVector listColVec = (ListColumnVector) colVector;
        listColVec.offsets[vectorPos] = listColVec.childCount;
        listColVec.lengths[vectorPos] = list.size();

        TypeDescription listType = type.getChildren().get(0);
        for (Object listItem : list) {
          addToVector(listType, listColVec.child, avroSchema.getElementType(), listItem, listColVec.childCount++);
        }
        break;
      case MAP:
        Map<String, ?> mapValue = (Map<String, ?>) value;

        MapColumnVector mapColumnVector = (MapColumnVector) colVector;
        mapColumnVector.offsets[vectorPos] = mapColumnVector.childCount;
        mapColumnVector.lengths[vectorPos] = mapValue.size();

        // keys are always strings
        Schema keySchema = Schema.create(Schema.Type.STRING);
        for (Map.Entry<String, ?> entry : mapValue.entrySet()) {
          addToVector(
              type.getChildren().get(0),
              mapColumnVector.keys,
              keySchema,
              entry.getKey(),
              mapColumnVector.childCount
          );

          addToVector(
              type.getChildren().get(1),
              mapColumnVector.values,
              avroSchema.getValueType(),
              entry.getValue(),
              mapColumnVector.childCount
          );

          mapColumnVector.childCount++;
        }

        break;
      case STRUCT:
        StructColumnVector structColVec = (StructColumnVector) colVector;

        GenericData.Record record = (GenericData.Record) value;

        for (int i = 0; i < type.getFieldNames().size(); i++) {
          String fieldName = type.getFieldNames().get(i);
          Object fieldValue = record.get(fieldName);
          TypeDescription fieldType = type.getChildren().get(i);
          addToVector(fieldType, structColVec.fields[i], avroSchema.getFields().get(i).schema(), fieldValue, vectorPos);
        }

        break;
      case UNION:
        UnionColumnVector unionColVec = (UnionColumnVector) colVector;

        List<TypeDescription> childTypes = type.getChildren();
        boolean added = addUnionValue(unionColVec, childTypes, avroSchema, value, vectorPos);

        if (!added) {
          throw new IllegalStateException(String.format(
              "Failed to add value %s to union with type %s",
              value == null ? "null" : value.toString(),
              type.toString()
          ));
        }

        break;
      default:
        throw new IllegalArgumentException("Invalid TypeDescription " + type.toString() + ".");
    }
  }

  /**
   * Match value with its ORC type and add to the union vector at a given position.
   *
   * @param unionVector       The vector to store value.
   * @param unionChildTypes   All possible types for the value Object.
   * @param avroSchema        Avro union schema for the value Object.
   * @param value             Object to be added to the unionVector
   * @param vectorPos         The position in the vector where value will be stored at.
   * @return                  succeeded or failed
   */
  public static boolean addUnionValue(
      UnionColumnVector unionVector,
      List<TypeDescription> unionChildTypes,
      Schema avroSchema,
      Object value,
      int vectorPos
  ) {
    int matchIndex = -1;
    TypeDescription matchType = null;
    Object matchValue = null;

    for (int t = 0; t < unionChildTypes.size(); t++) {
      TypeDescription childType = unionChildTypes.get(t);
      boolean matches = false;

      switch (childType.getCategory()) {
        case BOOLEAN:
          matches = value instanceof Boolean;
          break;
        case BYTE:
          matches = value instanceof Byte;
          break;
        case SHORT:
          matches = value instanceof Short;
          break;
        case INT:
          matches = value instanceof Integer;
          break;
        case LONG:
          matches = value instanceof Long;
          break;
        case FLOAT:
          matches = value instanceof Float;
          break;
        case DOUBLE:
          matches = value instanceof Double;
          break;
        case STRING:
        case VARCHAR:
        case CHAR:
          if (value instanceof String) {
            matches = true;
            matchValue = ((String) value).getBytes(StandardCharsets.UTF_8);
          } else if (value instanceof Utf8) {
            matches = true;
            matchValue = ((Utf8) value).getBytes();
          }
          break;
        case DATE:
          matches = value instanceof Date;
          break;
        case TIMESTAMP:
          matches = value instanceof Timestamp;
          break;
        case BINARY:
          matches = value instanceof byte[] || value instanceof GenericData.Fixed;
          break;
        case DECIMAL:
          matches = value instanceof BigDecimal;
          break;
        case LIST:
          matches = value instanceof List;
          break;
        case MAP:
          matches = value instanceof Map;
          break;
        case STRUCT:
          throw new UnsupportedOperationException("Cannot handle STRUCT within UNION.");
        case UNION:
          List<TypeDescription> children = childType.getChildren();
          if (value == null) {
            matches = children == null || children.size() == 0;
          } else {
            matches = addUnionValue(unionVector, children, avroSchema, value, vectorPos);
          }
          break;
        default:
          throw new IllegalArgumentException("Invalid TypeDescription " + childType.getCategory().toString() + ".");
      }

      if (matches) {
        matchIndex = t;
        matchType = childType;
        break;
      }
    }

    if (value == null && matchValue != null) {
      value = matchValue;
    }

    if (matchIndex >= 0) {
      unionVector.tags[vectorPos] = matchIndex;
      if (value == null) {
        unionVector.isNull[vectorPos] = true;
        unionVector.noNulls = false;
      } else {
        addToVector(matchType, unionVector.fields[matchIndex], avroSchema.getTypes().get(matchIndex), value, vectorPos);
      }
      return true;
    } else {
      return false;
    }
  }

  /**
   * Read the Column vector at a given position conforming to a given ORC schema.
   *
   * @param type        ORC schema of the object to read.
   * @param colVector   The column vector to read.
   * @param avroSchema  Avro schema of the object to read.
   *                    Only used to check logical types for timestamp unit conversion.
   * @param vectorPos   The position in the vector where the value to read is stored at.
   * @return            The object being read.
   */
  public static Object readFromVector(TypeDescription type, ColumnVector colVector, Schema avroSchema, int vectorPos) {

    if (colVector.isRepeating) {
      vectorPos = 0;
    }

    if (colVector.isNull[vectorPos]) {
      return null;
    }

    if (avroSchema.getType().equals(Schema.Type.UNION)) {
      avroSchema = getActualSchemaType(avroSchema);
    }
    LogicalType logicalType = avroSchema != null ? avroSchema.getLogicalType() : null;

    switch (type.getCategory()) {
      case BOOLEAN:
        return ((LongColumnVector) colVector).vector[vectorPos] != 0;
      case BYTE:
        return (byte) ((LongColumnVector) colVector).vector[vectorPos];
      case SHORT:
        return (short) ((LongColumnVector) colVector).vector[vectorPos];
      case INT:
        return (int) ((LongColumnVector) colVector).vector[vectorPos];
      case LONG:
        return ((LongColumnVector) colVector).vector[vectorPos];
      case FLOAT:
        return (float) ((DoubleColumnVector) colVector).vector[vectorPos];
      case DOUBLE:
        return ((DoubleColumnVector) colVector).vector[vectorPos];
      case VARCHAR:
      case CHAR:
        int maxLength = type.getMaxLength();
        String result = ((BytesColumnVector) colVector).toString(vectorPos);
        if (result.length() <= maxLength) {
          return result;
        } else {
          throw new HoodieIOException("CHAR/VARCHAR has length " + result.length() + " greater than Max Length allowed");
        }
      case STRING:
        String stringType = avroSchema.getProp(GenericData.STRING_PROP);
        if (stringType == null || !stringType.equals(StringType.String)) {
          int stringLength = ((BytesColumnVector) colVector).length[vectorPos];
          int stringOffset = ((BytesColumnVector) colVector).start[vectorPos];
          byte[] stringBytes = new byte[stringLength];
          System.arraycopy(((BytesColumnVector) colVector).vector[vectorPos], stringOffset, stringBytes, 0, stringLength);
          return new Utf8(stringBytes);
        } else {
          return ((BytesColumnVector) colVector).toString(vectorPos);
        }
      case DATE:
        // convert to daysSinceEpoch for LogicalType.Date
        return (int) ((LongColumnVector) colVector).vector[vectorPos];
      case TIMESTAMP:
        // The unit of time in ORC is millis. Convert (time,nanos) to the desired unit per logicalType
        long time = ((TimestampColumnVector) colVector).time[vectorPos];
        int nanos = ((TimestampColumnVector) colVector).nanos[vectorPos];
        if (logicalType instanceof LogicalTypes.TimestampMillis) {
          return time;
        } else if (logicalType instanceof LogicalTypes.TimestampMicros) {
          return time * MICROS_PER_MILLI + nanos / NANOS_PER_MICRO;
        } else {
          return ((TimestampColumnVector) colVector).getTimestampAsLong(vectorPos);
        }
      case BINARY:
        int binaryLength = ((BytesColumnVector) colVector).length[vectorPos];
        int binaryOffset = ((BytesColumnVector) colVector).start[vectorPos];
        byte[] binaryBytes = new byte[binaryLength];
        System.arraycopy(((BytesColumnVector) colVector).vector[vectorPos], binaryOffset, binaryBytes, 0, binaryLength);
        // return a ByteBuffer to be consistent with AvroRecordConverter
        return ByteBuffer.wrap(binaryBytes);
      case DECIMAL:
        // HiveDecimal always ignores trailing zeros, thus modifies the scale implicitly,
        // therefore, the scale must be enforced here.
        BigDecimal bigDecimal = ((DecimalColumnVector) colVector).vector[vectorPos]
            .getHiveDecimal().bigDecimalValue()
            .setScale(((LogicalTypes.Decimal) logicalType).getScale());
        Schema.Type baseType = avroSchema.getType();
        if (baseType.equals(Schema.Type.FIXED)) {
          return new Conversions.DecimalConversion().toFixed(bigDecimal, avroSchema, logicalType);
        } else if (baseType.equals(Schema.Type.BYTES)) {
          return bigDecimal.unscaledValue().toByteArray();
        } else {
          throw new HoodieIOException(baseType.getName() + "is not a valid type for LogicalTypes.DECIMAL.");
        }
      case LIST:
        ArrayList<Object> list = new ArrayList<>();
        ListColumnVector listVector = (ListColumnVector) colVector;
        int listLength = (int) listVector.lengths[vectorPos];
        int listOffset = (int) listVector.offsets[vectorPos];
        list.ensureCapacity(listLength);
        TypeDescription childType = type.getChildren().get(0);
        for (int i = 0; i < listLength; i++) {
          list.add(readFromVector(childType, listVector.child, avroSchema.getElementType(), listOffset + i));
        }
        return list;
      case MAP:
        Map<String, Object> map = new HashMap<String, Object>();
        MapColumnVector mapVector = (MapColumnVector) colVector;
        int mapLength = (int) mapVector.lengths[vectorPos];
        int mapOffset = (int) mapVector.offsets[vectorPos];
        // keys are always strings for maps in Avro
        Schema keySchema = Schema.create(Schema.Type.STRING);
        for (int i = 0; i < mapLength; i++) {
          map.put(
              readFromVector(type.getChildren().get(0), mapVector.keys, keySchema, i + mapOffset).toString(),
              readFromVector(type.getChildren().get(1), mapVector.values,
                  avroSchema.getValueType(), i + mapOffset));
        }
        return map;
      case STRUCT:
        StructColumnVector structVector = (StructColumnVector) colVector;
        List<TypeDescription> children = type.getChildren();
        GenericData.Record record = new GenericData.Record(avroSchema);
        for (int i = 0; i < children.size(); i++) {
          record.put(i, readFromVector(children.get(i), structVector.fields[i],
              avroSchema.getFields().get(i).schema(), vectorPos));
        }
        return record;
      case UNION:
        UnionColumnVector unionVector = (UnionColumnVector) colVector;
        int tag = unionVector.tags[vectorPos];
        ColumnVector fieldVector = unionVector.fields[tag];
        return readFromVector(type.getChildren().get(tag), fieldVector, avroSchema.getTypes().get(tag), vectorPos);
      default:
        throw new HoodieIOException("Unrecognized TypeDescription " + type.toString());
    }
  }

  public static TypeDescription createOrcSchema(Schema avroSchema) {

    LogicalType logicalType = avroSchema.getLogicalType();

    if (logicalType != null) {
      if (logicalType instanceof LogicalTypes.Decimal) {
        return TypeDescription.createDecimal()
            .withPrecision(((LogicalTypes.Decimal) logicalType).getPrecision())
            .withScale(((LogicalTypes.Decimal) logicalType).getScale());
      } else if (logicalType instanceof LogicalTypes.Date) {
        // The date logical type represents a date within the calendar, with no reference to a particular time zone
        // or time of day.
        //
        // A date logical type annotates an Avro int, where the int stores the number of days from the unix epoch, 1
        // January 1970 (ISO calendar).
        return TypeDescription.createDate();
      } else if (logicalType instanceof LogicalTypes.TimeMillis) {
        // The time-millis logical type represents a time of day, with no reference to a particular calendar, time
        // zone or date, with a precision of one millisecond.
        //
        // A time-millis logical type annotates an Avro int, where the int stores the number of milliseconds after
        // midnight, 00:00:00.000.
        return TypeDescription.createInt();
      } else if (logicalType instanceof LogicalTypes.TimeMicros) {
        // The time-micros logical type represents a time of day, with no reference to a particular calendar, time
        // zone or date, with a precision of one microsecond.
        //
        // A time-micros logical type annotates an Avro long, where the long stores the number of microseconds after
        // midnight, 00:00:00.000000.
        return TypeDescription.createLong();
      } else if (logicalType instanceof LogicalTypes.TimestampMillis) {
        // The timestamp-millis logical type represents an instant on the global timeline, independent of a
        // particular time zone or calendar, with a precision of one millisecond.
        //
        // A timestamp-millis logical type annotates an Avro long, where the long stores the number of milliseconds
        // from the unix epoch, 1 January 1970 00:00:00.000 UTC.
        return TypeDescription.createTimestamp();
      } else if (logicalType instanceof LogicalTypes.TimestampMicros) {
        // The timestamp-micros logical type represents an instant on the global timeline, independent of a
        // particular time zone or calendar, with a precision of one microsecond.
        //
        // A timestamp-micros logical type annotates an Avro long, where the long stores the number of microseconds
        // from the unix epoch, 1 January 1970 00:00:00.000000 UTC.
        return TypeDescription.createTimestamp();
      }
    }

    final Schema.Type type = avroSchema.getType();
    switch (type) {
      case NULL:
        // empty union represents null type
        final TypeDescription nullUnion = TypeDescription.createUnion();
        return nullUnion;
      case LONG:
        return TypeDescription.createLong();
      case INT:
        return TypeDescription.createInt();
      case BYTES:
        return TypeDescription.createBinary();
      case ARRAY:
        return TypeDescription.createList(createOrcSchema(avroSchema.getElementType()));
      case RECORD:
        final TypeDescription recordStruct = TypeDescription.createStruct();
        for (Schema.Field field : avroSchema.getFields()) {
          final Schema fieldSchema = field.schema();
          final TypeDescription fieldType = createOrcSchema(fieldSchema);
          if (fieldType != null) {
            recordStruct.addField(field.name(), fieldType);
          }
        }
        return recordStruct;
      case MAP:
        return TypeDescription.createMap(
            // in Avro maps, keys are always strings
            TypeDescription.createString(),
            createOrcSchema(avroSchema.getValueType())
        );
      case UNION:
        final List<Schema> nonNullMembers = avroSchema.getTypes().stream().filter(
            schema -> !Schema.Type.NULL.equals(schema.getType())
        ).collect(Collectors.toList());

        if (nonNullMembers.isEmpty()) {
          // no non-null union members; represent as an ORC empty union
          return TypeDescription.createUnion();
        } else if (nonNullMembers.size() == 1) {
          // a single non-null union member
          // this is how Avro represents "nullable" types; as a union of the NULL type with another
          // since ORC already supports nullability of all types, just use the child type directly
          return createOrcSchema(nonNullMembers.get(0));
        } else {
          // more than one non-null type; represent as an actual ORC union of them
          final TypeDescription union = TypeDescription.createUnion();
          for (final Schema childSchema : nonNullMembers) {
            union.addUnionChild(createOrcSchema(childSchema));
          }
          return union;
        }
      case STRING:
        return TypeDescription.createString();
      case FLOAT:
        return TypeDescription.createFloat();
      case DOUBLE:
        return TypeDescription.createDouble();
      case BOOLEAN:
        return TypeDescription.createBoolean();
      case ENUM:
        // represent as String for now
        return TypeDescription.createString();
      case FIXED:
        return TypeDescription.createBinary();
      default:
        throw new IllegalStateException(String.format("Unrecognized Avro type: %s", type.getName()));
    }
  }

  public static Schema createAvroSchema(TypeDescription orcSchema) {
    switch (orcSchema.getCategory()) {
      case BOOLEAN:
        return Schema.create(Schema.Type.BOOLEAN);
      case BYTE:
        // tinyint (8 bit), use int to hold it
        return Schema.create(Schema.Type.INT);
      case SHORT:
        // smallint (16 bit), use int to hold it
        return Schema.create(Schema.Type.INT);
      case INT:
        // the Avro logical type could be AvroTypeUtil.LOGICAL_TYPE_TIME_MILLIS, but there is no way to distinguish
        return Schema.create(Schema.Type.INT);
      case LONG:
        // the Avro logical type could be AvroTypeUtil.LOGICAL_TYPE_TIME_MICROS, but there is no way to distinguish
        return Schema.create(Schema.Type.LONG);
      case FLOAT:
        return Schema.create(Schema.Type.FLOAT);
      case DOUBLE:
        return Schema.create(Schema.Type.DOUBLE);
      case VARCHAR:
      case CHAR:
      case STRING:
        return Schema.create(Schema.Type.STRING);
      case DATE:
        Schema date = Schema.create(Schema.Type.INT);
        LogicalTypes.date().addToSchema(date);
        return date;
      case TIMESTAMP:
        // Cannot distinguish between TIMESTAMP_MILLIS and TIMESTAMP_MICROS
        // Assume TIMESTAMP_MILLIS because Timestamp in ORC is in millis
        Schema timestamp = Schema.create(Schema.Type.LONG);
        LogicalTypes.timestampMillis().addToSchema(timestamp);
        return timestamp;
      case BINARY:
        return Schema.create(Schema.Type.BYTES);
      case DECIMAL:
        Schema decimal = Schema.create(Schema.Type.BYTES);
        LogicalTypes.decimal(orcSchema.getPrecision(), orcSchema.getScale()).addToSchema(decimal);
        return decimal;
      case LIST:
        return Schema.createArray(createAvroSchema(orcSchema.getChildren().get(0)));
      case MAP:
        return Schema.createMap(createAvroSchema(orcSchema.getChildren().get(1)));
      case STRUCT:
        List<Field> childFields = new ArrayList<>();
        for (int i = 0; i < orcSchema.getChildren().size(); i++) {
          TypeDescription childType = orcSchema.getChildren().get(i);
          String childName = orcSchema.getFieldNames().get(i);
          childFields.add(new Field(childName, createAvroSchema(childType), "", null));
        }
        return Schema.createRecord(childFields);
      case UNION:
        return Schema.createUnion(orcSchema.getChildren().stream()
            .map(AvroOrcUtils::createAvroSchema)
            .collect(Collectors.toList()));
      default:
        throw new IllegalStateException(String.format("Unrecognized ORC type: %s", orcSchema.getCategory().getName()));
    }
  }

  public static Schema createAvroSchemaWithDefaultValue(TypeDescription orcSchema, String recordName, String namespace, boolean nullable) {
    Schema avroSchema = createAvroSchemaWithNamespace(orcSchema,recordName,namespace);
    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    List<Field> fieldList = avroSchema.getFields();
    for (Field field : fieldList) {
      Schema fieldSchema = field.schema();
      Schema nullableSchema = Schema.createUnion(Schema.create(Schema.Type.NULL),fieldSchema);
      if (nullable) {
        fields.add(new Schema.Field(field.name(), nullableSchema, null, NULL_VALUE));
      } else {
        fields.add(new Schema.Field(field.name(), fieldSchema, null, (Object) null));
      }
    }
    Schema schema = Schema.createRecord(recordName, null, null, false);
    schema.setFields(fields);
    return schema;
  }

  private static Schema createAvroSchemaWithNamespace(TypeDescription orcSchema, String recordName, String namespace) {
    switch (orcSchema.getCategory()) {
      case BOOLEAN:
        return Schema.create(Schema.Type.BOOLEAN);
      case BYTE:
        // tinyint (8 bit), use int to hold it
        return Schema.create(Schema.Type.INT);
      case SHORT:
        // smallint (16 bit), use int to hold it
        return Schema.create(Schema.Type.INT);
      case INT:
        // the Avro logical type could be AvroTypeUtil.LOGICAL_TYPE_TIME_MILLIS, but there is no way to distinguish
        return Schema.create(Schema.Type.INT);
      case LONG:
        // the Avro logical type could be AvroTypeUtil.LOGICAL_TYPE_TIME_MICROS, but there is no way to distinguish
        return Schema.create(Schema.Type.LONG);
      case FLOAT:
        return Schema.create(Schema.Type.FLOAT);
      case DOUBLE:
        return Schema.create(Schema.Type.DOUBLE);
      case VARCHAR:
      case CHAR:
      case STRING:
        return Schema.create(Schema.Type.STRING);
      case DATE:
        Schema date = Schema.create(Schema.Type.INT);
        LogicalTypes.date().addToSchema(date);
        return date;
      case TIMESTAMP:
        Schema timestamp = Schema.create(Schema.Type.LONG);
        LogicalTypes.timestampMillis().addToSchema(timestamp);
        return timestamp;
      case BINARY:
        return Schema.create(Schema.Type.BYTES);
      case DECIMAL:
        Schema decimal = Schema.create(Schema.Type.BYTES);
        LogicalTypes.decimal(orcSchema.getPrecision(), orcSchema.getScale()).addToSchema(decimal);
        return decimal;
      case LIST:
        return Schema.createArray(createAvroSchemaWithNamespace(orcSchema.getChildren().get(0),recordName,""));
      case MAP:
        return Schema.createMap(createAvroSchemaWithNamespace(orcSchema.getChildren().get(1),recordName,""));
      case STRUCT:
        List<Field> childFields = new ArrayList<>();
        for (int i = 0; i < orcSchema.getChildren().size(); i++) {
          TypeDescription childType = orcSchema.getChildren().get(i);
          String childName = orcSchema.getFieldNames().get(i);
          childFields.add(new Field(childName, createAvroSchemaWithNamespace(childType,childName,""), null, null));
        }
        return Schema.createRecord(recordName,null, namespace,false,childFields);
      default:
        throw new IllegalStateException(String.format("Unrecognized ORC type: %s", orcSchema.getCategory().getName()));

    }
  }

  /**
   * Returns the actual schema of a field.
   *
   * All types in ORC is nullable whereas Avro uses a union that contains the NULL type to imply
   * the nullability of an Avro type. To achieve consistency between the Avro and ORC schema,
   * non-NULL types are extracted from the union type.
   * @param unionSchema       A schema of union type.
   * @return  An Avro schema that is either NULL or a UNION without NULL fields.
   */
  private static Schema getActualSchemaType(Schema unionSchema) {
    final List<Schema> nonNullMembers = unionSchema.getTypes().stream().filter(
        schema -> !Schema.Type.NULL.equals(schema.getType())
    ).collect(Collectors.toList());
    if (nonNullMembers.isEmpty()) {
      return Schema.create(Schema.Type.NULL);
    } else if (nonNullMembers.size() == 1) {
      return nonNullMembers.get(0);
    } else {
      return Schema.createUnion(nonNullMembers);
    }
  }
}

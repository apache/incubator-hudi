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

package org.apache.hudi

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.util

import org.apache.avro.Conversions.DecimalConversion
import org.apache.avro.LogicalTypes.{TimestampMicros, TimestampMillis}
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData.{Fixed, Record}
import org.apache.avro.generic.{GenericData, GenericFixed, GenericRecord}
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.apache.spark.sql.Row
import org.apache.spark.sql.avro.{IncompatibleSchemaException, SchemaConverters}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.hudi.AvroConversionUtils._

import scala.collection.JavaConverters._

object AvroConversionHelper {

  private def createDecimal(decimal: java.math.BigDecimal, precision: Int, scale: Int): Decimal = {
    if (precision <= Decimal.MAX_LONG_DIGITS) {
      // Constructs a `Decimal` with an unscaled `Long` value if possible.
      Decimal(decimal.unscaledValue().longValue(), precision, scale)
    } else {
      // Otherwise, resorts to an unscaled `BigInteger` instead.
      Decimal(decimal, precision, scale)
    }
  }

  /**
    *
    * Returns a converter function to convert row in avro format to GenericRow of catalyst.
    *
    * @param sourceAvroSchema Source schema before conversion inferred from avro file by passed in
    *                         by user.
    * @param targetSqlType    Target catalyst sql type after the conversion.
    * @return returns a converter function to convert row in avro format to GenericRow of catalyst.
    */
  def createConverterToRow(sourceAvroSchema: Schema,
                           targetSqlType: DataType): AnyRef => AnyRef = {

    def createConverter(avroSchema: Schema, sqlType: DataType, path: List[String]): AnyRef => AnyRef = {
      val avroType = avroSchema.getType
      (sqlType, avroType) match {
        // Avro strings are in Utf8, so we have to call toString on them
        case (StringType, STRING) | (StringType, ENUM) =>
          (item: AnyRef) => if (item == null) null else item.toString
        // Byte arrays are reused by avro, so we have to make a copy of them.
        case (IntegerType, INT) | (BooleanType, BOOLEAN) | (DoubleType, DOUBLE) |
             (FloatType, FLOAT) | (LongType, LONG) =>
          identity
        case (BinaryType, FIXED) =>
          (item: AnyRef) =>
            if (item == null) {
              null
            } else {
              item.asInstanceOf[Fixed].bytes().clone()
            }
        case (BinaryType, BYTES) =>
          (item: AnyRef) =>
            if (item == null) {
              null
            } else {
              val byteBuffer = item.asInstanceOf[ByteBuffer]
              val bytes = new Array[Byte](byteBuffer.remaining)
              byteBuffer.get(bytes)
              bytes
            }
        case (d: DecimalType, FIXED) =>
          (item: AnyRef) =>
            if (item == null) {
              null
            } else {
              val decimalConversion = new DecimalConversion
              val bigDecimal = decimalConversion.fromFixed(item.asInstanceOf[GenericFixed], avroSchema,
                LogicalTypes.decimal(d.precision, d.scale))
              createDecimal(bigDecimal, d.precision, d.scale)
            }
        case (d: DecimalType, BYTES) =>
          (item: AnyRef) =>
            if (item == null) {
              null
            } else {
              val decimalConversion = new DecimalConversion
              val bigDecimal = decimalConversion.fromBytes(item.asInstanceOf[ByteBuffer], avroSchema,
                LogicalTypes.decimal(d.precision, d.scale))
              createDecimal(bigDecimal, d.precision, d.scale)
            }
        case (DateType, INT) =>
          (item: AnyRef) =>
            if (item == null) {
              null
            } else {
              item match {
                case integer: Integer => DateTimeUtils.toJavaDate(integer)
                case _ => new Date(item.asInstanceOf[Long])
              }
            }
        case (TimestampType, LONG) =>
          (item: AnyRef) =>
            if (item == null) {
              null
            } else {
              avroSchema.getLogicalType match {
                case _: TimestampMillis =>
                  new Timestamp(item.asInstanceOf[Long])
                case _: TimestampMicros =>
                  new Timestamp(item.asInstanceOf[Long] / 1000)
                case null =>
                  new Timestamp(item.asInstanceOf[Long])
                case other =>
                  throw new IncompatibleSchemaException(
                    s"Cannot convert Avro logical type $other to Catalyst Timestamp type.")
              }
            }
        case (struct: StructType, RECORD) =>
          val length = struct.fields.length
          val converters = new Array[AnyRef => AnyRef](length)
          val avroFieldNames = new Array[String](length)
          var i = 0
          while (i < length) {
            val sqlField = struct.fields(i)
            val avroField = avroSchema.getField(sqlField.name)
            if (avroField != null) {
              val converter = createConverter(avroField.schema(), sqlField.dataType,
                path :+ sqlField.name)
              converters(i) = converter
              avroFieldNames(i) = avroField.name()
            } else if (!sqlField.nullable) {
              throw new IncompatibleSchemaException(
                s"Cannot find non-nullable field ${sqlField.name} at path ${path.mkString(".")} " +
                  "in Avro schema\n" +
                  s"Source Avro schema: $sourceAvroSchema.\n" +
                  s"Target Catalyst type: $targetSqlType")
            }
            i += 1
          }

          (item: AnyRef) => {
            if (item == null) {
              null
            } else {
              val record = item.asInstanceOf[GenericRecord]

              val result = new Array[Any](length)
              var i = 0

              while (i < converters.length) {
                if (converters(i) != null) {
                  val converter = converters(i)
                  result(i) = converter(record.get(avroFieldNames(i)))
                }
                i += 1
              }
              new GenericRow(result)
            }
          }
        case (arrayType: ArrayType, ARRAY) =>
          val elementConverter = createConverter(avroSchema.getElementType, arrayType.elementType,
            path)
          val allowsNull = arrayType.containsNull
          (item: AnyRef) => {
            if (item == null) {
              null
            } else {
              item.asInstanceOf[java.lang.Iterable[AnyRef]].asScala.map { element =>
                if (element == null && !allowsNull) {
                  throw new RuntimeException(s"Array value at path ${path.mkString(".")} is not " +
                    "allowed to be null")
                } else {
                  elementConverter(element)
                }
              }
            }
          }
        case (mapType: MapType, MAP) if mapType.keyType == StringType =>
          val valueConverter = createConverter(avroSchema.getValueType, mapType.valueType, path)
          val allowsNull = mapType.valueContainsNull
          (item: AnyRef) => {
            if (item == null) {
              null
            } else {
              item.asInstanceOf[java.util.Map[AnyRef, AnyRef]].asScala.map { x =>
                if (x._2 == null && !allowsNull) {
                  throw new RuntimeException(s"Map value at path ${path.mkString(".")} is not " +
                    "allowed to be null")
                } else {
                  (x._1.toString, valueConverter(x._2))
                }
              }.toMap
            }
          }
        case (sqlType, UNION) =>
          if (avroSchema.getTypes.asScala.exists(_.getType == NULL)) {
            val remainingUnionTypes = avroSchema.getTypes.asScala.filterNot(_.getType == NULL)
            if (remainingUnionTypes.size == 1) {
              createConverter(remainingUnionTypes.head, sqlType, path)
            } else {
              createConverter(Schema.createUnion(remainingUnionTypes.asJava), sqlType, path)
            }
          } else avroSchema.getTypes.asScala.map(_.getType) match {
            case Seq(_) => createConverter(avroSchema.getTypes.get(0), sqlType, path)
            case Seq(a, b) if Set(a, b) == Set(INT, LONG) && sqlType == LongType =>
              (item: AnyRef) => {
                item match {
                  case null => null
                  case l: java.lang.Long => l
                  case i: java.lang.Integer => new java.lang.Long(i.longValue())
                }
              }
            case Seq(a, b) if Set(a, b) == Set(FLOAT, DOUBLE) && sqlType == DoubleType =>
              (item: AnyRef) => {
                item match {
                  case null => null
                  case d: java.lang.Double => d
                  case f: java.lang.Float => new java.lang.Double(f.doubleValue())
                }
              }
            case other =>
              sqlType match {
                case t: StructType if t.fields.length == avroSchema.getTypes.size =>
                  val fieldConverters = t.fields.zip(avroSchema.getTypes.asScala).map {
                    case (field, schema) =>
                      createConverter(schema, field.dataType, path :+ field.name)
                  }

                  (item: AnyRef) =>
                    if (item == null) {
                      null
                    } else {
                      val i = GenericData.get().resolveUnion(avroSchema, item)
                      val converted = new Array[Any](fieldConverters.length)
                      converted(i) = fieldConverters(i)(item)
                      new GenericRow(converted)
                    }
                case _ => throw new IncompatibleSchemaException(
                  s"Cannot convert Avro schema to catalyst type because schema at path " +
                    s"${path.mkString(".")} is not compatible " +
                    s"(avroType = $other, sqlType = $sqlType). \n" +
                    s"Source Avro schema: $sourceAvroSchema.\n" +
                    s"Target Catalyst type: $targetSqlType")
              }
          }
        case (left, right) =>
          throw new IncompatibleSchemaException(
            s"Cannot convert Avro schema to catalyst type because schema at path " +
              s"${path.mkString(".")} is not compatible (avroType = $left, sqlType = $right). \n" +
              s"Source Avro schema: $sourceAvroSchema.\n" +
              s"Target Catalyst type: $targetSqlType")
      }
    }

    createConverter(sourceAvroSchema, targetSqlType, List.empty[String])
  }

  def createConverterToAvro(dataType: DataType,
                            structName: String,
                            recordNamespace: String): Any => Any = {
    dataType match {
      case BinaryType => (item: Any) =>
        item match {
          case null => null
          case bytes: Array[Byte] => ByteBuffer.wrap(bytes)
        }
      case IntegerType | LongType |
           FloatType | DoubleType | StringType | BooleanType => identity
      case ByteType => (item: Any) =>
        if (item == null) null else item.asInstanceOf[Byte].intValue
      case ShortType => (item: Any) =>
        if (item == null) null else item.asInstanceOf[Short].intValue
      case dec: DecimalType =>
        val schema = SchemaConverters.toAvroType(dec, nullable = false, structName, recordNamespace)
        (item: Any) => {
          Option(item).map { _ =>
            val bigDecimalValue = item.asInstanceOf[java.math.BigDecimal]
            val decimalConversions = new DecimalConversion()
            decimalConversions.toFixed(bigDecimalValue, schema, LogicalTypes.decimal(dec.precision, dec.scale))
          }.orNull
        }
      case TimestampType => (item: Any) =>
        // Convert time to microseconds since spark-avro by default converts TimestampType to
        // Avro Logical TimestampMicros
        Option(item).map(_.asInstanceOf[Timestamp].getTime * 1000).orNull
      case DateType => (item: Any) =>
        Option(item).map(_.asInstanceOf[Date].toLocalDate.toEpochDay.toInt).orNull
      case ArrayType(elementType, _) =>
        val elementConverter = createConverterToAvro(
          elementType,
          structName,
          recordNamespace)
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val sourceArray = item.asInstanceOf[Seq[Any]]
            val sourceArraySize = sourceArray.size
            val targetList = new util.ArrayList[Any](sourceArraySize)
            var idx = 0
            while (idx < sourceArraySize) {
              targetList.add(elementConverter(sourceArray(idx)))
              idx += 1
            }
            targetList
          }
        }
      case MapType(StringType, valueType, _) =>
        val valueConverter = createConverterToAvro(
          valueType,
          structName,
          recordNamespace)
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val javaMap = new util.HashMap[String, Any]()
            item.asInstanceOf[Map[String, Any]].foreach { case (key, value) =>
              javaMap.put(key, valueConverter(value))
            }
            javaMap
          }
        }
      case structType: StructType =>
        val schema: Schema = removeNamespaceFromFixedFields(convertStructTypeToAvroSchema(structType, structName, recordNamespace))
        val childNameSpace = if (recordNamespace != "") s"$recordNamespace.$structName" else structName
        val fieldConverters = structType.fields.map(field =>
          createConverterToAvro(
            field.dataType,
            field.name,
            childNameSpace))
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val record = new Record(schema)
            val convertersIterator = fieldConverters.iterator
            val fieldNamesIterator = dataType.asInstanceOf[StructType].fieldNames.iterator
            val rowIterator = item.asInstanceOf[Row].toSeq.iterator

            while (convertersIterator.hasNext) {
              val converter = convertersIterator.next()
              record.put(fieldNamesIterator.next(), converter(rowIterator.next()))
            }
            record
          }
        }
    }
  }

  /**
   * Remove namespace from fixed field.
   * org.apache.spark.sql.avro.SchemaConverters.toAvroType method adds namespace to fixed avro field
   * https://github.com/apache/spark/blob/master/external/avro/src/main/scala/org/apache/spark/sql/avro/SchemaConverters.scala#L177
   * So, we need to remove that namespace so that reader schema without namespace do not throw erorr like this one
   * org.apache.avro.AvroTypeException: Found hoodie.source.hoodie_source.height.fixed, expecting fixed
   *
   * @param schema Schema from which namespace needs to be removed for fixed fields
   * @return input schema with namespace removed for fixed fields, if any
   */
  def removeNamespaceFromFixedFields(schema: Schema): Schema  ={
    val fields = new util.ArrayList[Schema.Field]
    var isSchemaChanged = false

    import scala.collection.JavaConversions._

    for (field <- schema.getFields) {
      var fieldSchema = field.schema
      if (fieldSchema.getType.getName == "fixed" && fieldSchema.getLogicalType != null
        && fieldSchema.getLogicalType.getName == "decimal" && fieldSchema.getNamespace != "") {
        isSchemaChanged = true
        val name = fieldSchema.getName
        val avroType = fieldSchema.getLogicalType.asInstanceOf[LogicalTypes.Decimal]
        fieldSchema = avroType.addToSchema(SchemaBuilder.fixed(name).size(fieldSchema.getFixedSize))
      }
      fields.add(new Schema.Field(field.name, fieldSchema, field.doc, field.defaultVal))
    }

    if (isSchemaChanged) {
      Schema.createRecord(schema.getName, "", schema.getNamespace, false, fields)
    }
    else {
      schema
    }
  }
}

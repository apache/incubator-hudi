/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.command.payload

import java.util.{Base64, Properties}
import java.util.concurrent.Callable
import scala.collection.JavaConverters._
import com.google.common.cache.CacheBuilder
import org.apache.avro.Conversions.DecimalConversion
import org.apache.avro.Schema.Type
import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.generic.{GenericData, GenericRecord, IndexedRecord}
import org.apache.avro.util.Utf8
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.avro.HoodieAvroUtils.bytesToAvro
import org.apache.hudi.common.model.{DefaultHoodieRecordPayload, HoodiePayloadProps, HoodieRecord}
import org.apache.hudi.common.util.{ValidationUtils, Option => HOption}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.io.HoodieWriteHandle
import org.apache.hudi.sql.IExpressionEvaluator
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.hudi.SerDeUtils
import org.apache.spark.sql.hudi.command.payload.ExpressionPayload.getEvaluator
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ArrayBuffer

/**
 * A HoodieRecordPayload for MergeIntoHoodieTableCommand.
 * It will execute the condition and assignments expression in the
 * match and not-match actions and compute the final record to write.
 *
 * If there is no condition match the record, ExpressionPayload will return
 * a HoodieWriteHandle.IGNORE_RECORD, and the write handles will ignore this record.
 */
class ExpressionPayload(record: GenericRecord,
                        orderingVal: Comparable[_])
  extends DefaultHoodieRecordPayload(record, orderingVal) {

  def this(recordOpt: HOption[GenericRecord]) {
    this(recordOpt.orElse(null), 0)
  }

  /**
   * The schema of this table.
   */
  private var writeSchema: Schema = _

  override def combineAndGetUpdateValue(currentValue: IndexedRecord,
                                        schema: Schema): HOption[IndexedRecord] = {
    throw new IllegalStateException(s"Should not call this method for ${getClass.getCanonicalName}")
  }

  override def getInsertValue(schema: Schema): HOption[IndexedRecord] = {
    throw new IllegalStateException(s"Should not call this method for ${getClass.getCanonicalName}")
  }

  override def combineAndGetUpdateValue(targetRecord: IndexedRecord,
                                        schema: Schema, properties: Properties): HOption[IndexedRecord] = {
    val sourceRecord = bytesToAvro(recordBytes, schema)
    val joinSqlRecord = new SqlTypedRecord(joinRecord(sourceRecord, targetRecord))
    processMatchedRecord(joinSqlRecord, Some(targetRecord), properties)
  }

  /**
   * Process the matched record. Firstly test if the record matched any of the update-conditions,
   * if matched, return the update assignments result. Secondly, test if the record matched
   * delete-condition, if matched then return a delete record. Finally if no condition matched,
   * return a {@link HoodieWriteHandle.IGNORE_RECORD} which will be ignored by HoodieWriteHandle.
   * @param inputRecord  The input record to process.
   * @param targetRecord The origin exist record.
   * @param properties   The properties.
   * @return The result of the record to update or delete.
   */
  private def processMatchedRecord(inputRecord: SqlTypedRecord,
    targetRecord: Option[IndexedRecord], properties: Properties): HOption[IndexedRecord] = {
    // Process update
    val updateConditionAndAssignmentsText =
      properties.get(ExpressionPayload.PAYLOAD_UPDATE_CONDITION_AND_ASSIGNMENTS)
    assert(updateConditionAndAssignmentsText != null,
      s"${ExpressionPayload.PAYLOAD_UPDATE_CONDITION_AND_ASSIGNMENTS} have not set")

    var resultRecordOpt: HOption[IndexedRecord] = null

    // Get the Evaluator for each condition and update assignments.
    initWriteSchemaIfNeed(properties)
    val updateConditionAndAssignments = getEvaluator(updateConditionAndAssignmentsText.toString, writeSchema)
    for ((conditionEvaluator, assignmentEvaluator) <- updateConditionAndAssignments
         if resultRecordOpt == null) {
      val conditionVal = evaluate(conditionEvaluator, inputRecord).head.asInstanceOf[Boolean]
      // If the update condition matched  then execute assignment expression
      // to compute final record to update. We will return the first matched record.
      if (conditionVal) {
        val results = evaluate(assignmentEvaluator, inputRecord)
        val resultRecord = convertToRecord(results, writeSchema)

        if (targetRecord.isEmpty || needUpdatingPersistedRecord(targetRecord.get, resultRecord, properties)) {
          resultRecordOpt = HOption.of(resultRecord)
        } else {
          // if the PreCombine field value of targetRecord is greate
          // than the new incoming record, just keep the old record value.
          resultRecordOpt = HOption.of(targetRecord.get)
        }
      }
    }
    if (resultRecordOpt == null) {
      // Process delete
      val deleteConditionText = properties.get(ExpressionPayload.PAYLOAD_DELETE_CONDITION)
      if (deleteConditionText != null) {
        val deleteCondition = getEvaluator(deleteConditionText.toString, writeSchema).head._1
        val deleteConditionVal = evaluate(deleteCondition, inputRecord).head.asInstanceOf[Boolean]
        if (deleteConditionVal) {
          resultRecordOpt = HOption.empty()
        }
      }
    }
    if (resultRecordOpt == null) {
      // If there is no condition matched, just filter this record.
      // here we return a IGNORE_RECORD, HoodieMergeHandle will not handle it.
      HOption.of(HoodieWriteHandle.IGNORE_RECORD)
    } else {
      resultRecordOpt
    }
  }

  /**
   * Process the not-matched record. Test if the record matched any of insert-conditions,
   * if matched then return the result of insert-assignment. Or else return a
   * {@link HoodieWriteHandle.IGNORE_RECORD} which will be ignored by HoodieWriteHandle.
   *
   * @param inputRecord The input record to process.
   * @param properties  The properties.
   * @return The result of the record to insert.
   */
  private def processNotMatchedRecord(inputRecord: SqlTypedRecord, properties: Properties): HOption[IndexedRecord] = {
    val insertConditionAndAssignmentsText =
      properties.get(ExpressionPayload.PAYLOAD_INSERT_CONDITION_AND_ASSIGNMENTS)
    // Get the evaluator for each condition and insert assignment.
    initWriteSchemaIfNeed(properties)
    val insertConditionAndAssignments =
      ExpressionPayload.getEvaluator(insertConditionAndAssignmentsText.toString, writeSchema)
    var resultRecordOpt: HOption[IndexedRecord] = null
    for ((conditionEvaluator, assignmentEvaluator) <- insertConditionAndAssignments
         if resultRecordOpt == null) {
      val conditionVal = evaluate(conditionEvaluator, inputRecord).head.asInstanceOf[Boolean]
      // If matched the insert condition then execute the assignment expressions to compute the
      // result record. We will return the first matched record.
      if (conditionVal) {
        val results = evaluate(assignmentEvaluator, inputRecord)
        resultRecordOpt = HOption.of(convertToRecord(results, writeSchema))
      }
    }
    if (resultRecordOpt != null) {
      resultRecordOpt
    } else {
      // If there is no condition matched, just filter this record.
      // Here we return a IGNORE_RECORD, HoodieCreateHandle will not handle it.
      HOption.of(HoodieWriteHandle.IGNORE_RECORD)
    }
  }

  override def getInsertValue(schema: Schema, properties: Properties): HOption[IndexedRecord] = {
    val incomingRecord = bytesToAvro(recordBytes, schema)
    if (isDeleteRecord(incomingRecord)) {
      HOption.empty[IndexedRecord]()
    } else {
      val sqlTypedRecord = new SqlTypedRecord(incomingRecord)
      if (isMORTable(properties)) {
        // For the MOR table, both the matched and not-matched record will step into the getInsertValue() method.
        // We call the processMatchedRecord() method if current is a Update-Record to process
        // the matched record. Or else we call processNotMatchedRecord() method to process the not matched record.
        val isUpdateRecord = properties.getProperty(HoodiePayloadProps.PAYLOAD_IS_UPDATE_RECORD_FOR_MOR, "false").toBoolean
        if (isUpdateRecord) {
          processMatchedRecord(sqlTypedRecord, Option.empty, properties)
        } else {
          processNotMatchedRecord(sqlTypedRecord, properties)
        }
      } else {
        // For COW table, only the not-matched record will step into the getInsertValue method, So just call
        // the processNotMatchedRecord() here.
        processNotMatchedRecord(sqlTypedRecord, properties)
      }
    }
  }

  private def isMORTable(properties: Properties): Boolean = {
    properties.getProperty(TABLE_TYPE_OPT_KEY.key, null) == MOR_TABLE_TYPE_OPT_VAL
  }

  private def convertToRecord(values: Array[AnyRef], schema: Schema): IndexedRecord = {
    assert(values.length == schema.getFields.size())
    val writeRecord = new GenericData.Record(schema)
    for (i <- values.indices) {
      writeRecord.put(i, values(i))
    }
    writeRecord
  }

  /**
   * Init the table schema.
   */
  private def initWriteSchemaIfNeed(properties: Properties): Unit = {
    if (writeSchema == null) {
      ValidationUtils.checkArgument(properties.containsKey(HoodieWriteConfig.WRITE_SCHEMA_PROP.key),
        s"Missing ${HoodieWriteConfig.WRITE_SCHEMA_PROP.key}")
      writeSchema = new Schema.Parser().parse(properties.getProperty(HoodieWriteConfig.WRITE_SCHEMA_PROP.key))
    }
  }

  /**
   * Join the source record with the target record.
   *
   * @return
   */
  private def joinRecord(sourceRecord: IndexedRecord, targetRecord: IndexedRecord): IndexedRecord = {
    val leftSchema = sourceRecord.getSchema
    // the targetRecord is load from the disk, it contains the meta fields, so we remove it here
    val rightSchema = HoodieAvroUtils.removeMetadataFields(targetRecord.getSchema)
    val joinSchema = mergeSchema(leftSchema, rightSchema)

    val values = new ArrayBuffer[AnyRef]()
    for (i <- 0 until joinSchema.getFields.size()) {
      val value = if (i < leftSchema.getFields.size()) {
        sourceRecord.get(i)
      } else { // skip meta field
        targetRecord.get(i - leftSchema.getFields.size() + HoodieRecord.HOODIE_META_COLUMNS.size())
      }
      values += value
    }
    convertToRecord(values.toArray, joinSchema)
  }

  private def mergeSchema(a: Schema, b: Schema): Schema = {
    val mergedFields =
      a.getFields.asScala.map(field =>
        new Schema.Field("a_" + field.name,
          field.schema, field.doc, field.defaultVal, field.order)) ++
        b.getFields.asScala.map(field =>
          new Schema.Field("b_" + field.name,
            field.schema, field.doc, field.defaultVal, field.order))
    Schema.createRecord(a.getName, a.getDoc, a.getNamespace, a.isError, mergedFields.asJava)
  }

  private def evaluate(evaluator: IExpressionEvaluator, sqlTypedRecord: SqlTypedRecord): Array[Object] = {
    try evaluator.eval(sqlTypedRecord) catch {
      case e: Throwable =>
        throw new RuntimeException(s"Error in execute expression: ${e.getMessage}.\n${evaluator.getCode}", e)
    }
  }
}

object ExpressionPayload {

  /**
   * Property for pass the merge-into delete clause condition expresssion.
   */
  val PAYLOAD_DELETE_CONDITION = "hoodie.payload.delete.condition"

  /**
   * Property for pass the merge-into update clauses's condition and assignments.
   */
  val PAYLOAD_UPDATE_CONDITION_AND_ASSIGNMENTS = "hoodie.payload.update.condition.assignments"

  /**
   * Property for pass the merge-into insert clauses's condition and assignments.
   */
  val PAYLOAD_INSERT_CONDITION_AND_ASSIGNMENTS = "hoodie.payload.insert.condition.assignments"

  /**
   * A cache for the serializedConditionAssignments to the compiled class after CodeGen.
   * The Map[IExpressionEvaluator, IExpressionEvaluator] is the map of the condition expression
   * to the assignments expression.
   */
  private val cache = CacheBuilder.newBuilder()
    .maximumSize(1024)
    .build[String, Map[IExpressionEvaluator, IExpressionEvaluator]]()

  /**
   * Do the CodeGen for each condition and assignment expressions.We will cache it to reduce
   * the compile time for each method call.
   * @param serializedConditionAssignments
   * @return
   */
  def getEvaluator(
    serializedConditionAssignments: String, writeSchema: Schema): Map[IExpressionEvaluator, IExpressionEvaluator] = {
    cache.get(serializedConditionAssignments,
      new Callable[Map[IExpressionEvaluator, IExpressionEvaluator]] {

        override def call(): Map[IExpressionEvaluator, IExpressionEvaluator] = {
          val serializedBytes = Base64.getDecoder.decode(serializedConditionAssignments)
          val conditionAssignments = SerDeUtils.toObject(serializedBytes)
            .asInstanceOf[Map[Expression, Seq[Expression]]]
          // Do the CodeGen for condition expression and assignment expression
          conditionAssignments.map {
            case (condition, assignments) =>
              val conditionEvaluator = ExpressionCodeGen.doCodeGen(Seq(condition))
              val assignmentEvaluator = AvroTypeConvertEvaluator(ExpressionCodeGen.doCodeGen(assignments), writeSchema)
              conditionEvaluator -> assignmentEvaluator
          }
        }
      })
  }

  /**
   * A IExpressionEvaluator wrapped the base evaluator which convert the result of the base evaluator
   * to the avro typed-value.
   */
  case class AvroTypeConvertEvaluator(baseEvaluator: IExpressionEvaluator, writeSchema: Schema) extends IExpressionEvaluator {
    private lazy val decimalConversions = new DecimalConversion()

    /**
     * Convert to the avro typed-value.
     * e.g. convert UTF8String -> Utf8, Dicimal -> GenericFixed.
     */
    override def eval(record: IndexedRecord): Array[AnyRef] = {
      baseEvaluator.eval(record).zipWithIndex.map {
        case (s: UTF8String, _) => new Utf8(s.toString)
        case (d: Decimal, i) =>
          val schema = writeSchema.getFields.get(i).schema()
          val fixedSchema = if (schema.getType == Type.UNION) {
            schema.getTypes.asScala.filter(s => s.getType != Type.NULL).head
          } else {
            schema
          }
          decimalConversions.toFixed(d.toJavaBigDecimal, fixedSchema
            , LogicalTypes.decimal(d.precision, d.scale))
        case (o, _) => o
      }
    }

    override def getCode: String = baseEvaluator.getCode
  }
}


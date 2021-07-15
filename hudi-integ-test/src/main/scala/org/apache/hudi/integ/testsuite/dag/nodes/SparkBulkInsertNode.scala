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

package org.apache.hudi.integ.testsuite.dag.nodes

import org.apache.hudi.client.WriteStatus
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config
import org.apache.hudi.integ.testsuite.dag.ExecutionContext
import org.apache.hudi.{AvroConversionUtils, DataSourceWriteOptions}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

import scala.collection.JavaConverters._

/**
 * Spark datasource based bulk insert node
 * @param config1
 */
class SparkBulkInsertNode(config1: Config) extends DagNode[RDD[WriteStatus]] {

  config = config1

  /**
   * Execute the {@link DagNode}.
   *
   * @param context     The context needed for an execution of a node.
   * @param curItrCount iteration count for executing the node.
   * @throws Exception Thrown if the execution failed.
   */
  override def execute(context: ExecutionContext, curItrCount: Int): Unit = {
    if (!config.isDisableGenerate) {
      context.getDeltaGenerator().writeRecords(context.getDeltaGenerator().generateInserts(config)).count()
    }
    val inputDF = AvroConversionUtils.createDataFrame(context.getWriterContext.getHoodieTestSuiteWriter.getNextBatch,
      context.getWriterContext.getHoodieTestSuiteWriter.getSchema,
      context.getWriterContext.getSparkSession)
    val saveMode = if(curItrCount == 0) SaveMode.Overwrite else SaveMode.Append
    inputDF.write.format("hudi")
      .options(DataSourceWriteOptions.translateSqlOptions(context.getWriterContext.getProps.asScala.toMap))
      .option(DataSourceWriteOptions.TABLE_NAME_OPT_KEY, context.getHoodieTestSuiteWriter.getCfg.targetTableName)
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, context.getHoodieTestSuiteWriter.getCfg.tableType)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.ENABLE_ROW_WRITER_OPT_KEY, String.valueOf(config.enableRowWriting()))
      .option(DataSourceWriteOptions.COMMIT_METADATA_KEYPREFIX_OPT_KEY, "deltastreamer.checkpoint.key")
      .option("deltastreamer.checkpoint.key", context.getWriterContext.getHoodieTestSuiteWriter.getLastCheckpoint.orElse(""))
      .option(HoodieWriteConfig.TABLE_NAME, context.getHoodieTestSuiteWriter.getCfg.targetTableName)
      .mode(saveMode)
      .save(context.getHoodieTestSuiteWriter.getWriteConfig.getBasePath)
  }
}

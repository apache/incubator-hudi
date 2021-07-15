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

package org.apache.hudi.examples.spark;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.AvroOrcUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieBootstrapConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.examples.common.HoodieExampleDataGenerator;
import org.apache.hudi.examples.common.HoodieExampleSparkUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

import java.util.HashMap;
import java.util.Map;

public class HoodieSparkBootstrapExample {

    private static String tableType = HoodieTableType.MERGE_ON_READ.name();


    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: HoodieWriteClientExample <tablePath> <tableName>");
            System.exit(1);
        }
        String tablePath = args[0];
        String tableName = args[1];
        String fileFormat = args[2];
        String tableTy = args[3];

        if (tableTy.equals("MOR"))
            tableType = HoodieTableType.MERGE_ON_READ.name();
        else
            tableType = HoodieTableType.COPY_ON_WRITE.name();

        SparkConf sparkConf = HoodieExampleSparkUtils.defaultSparkConf("hoodie-client-example");

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .enableHiveSupport()
                .getOrCreate();


        Dataset df =  spark.emptyDataFrame();

        Map<String, String> opts = new HashMap<String,String>();
        opts.put("hoodie.table.base.file.format","ORC");

        df.write().format("hudi").option(HoodieWriteConfig.TABLE_NAME.key(), tableName)
                .option(DataSourceWriteOptions.OPERATION_OPT_KEY().key(), DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL())
                .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY().key(), "sno")
                .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY().key(), "observationdate")
                .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY().key(), "observationdate")
                .option(HoodieBootstrapConfig.HOODIE_BASE_FILE_FORMAT_PROP.key(), HoodieFileFormat.ORC.name())
                .option(HoodieBootstrapConfig.BOOTSTRAP_BASE_PATH_PROP.key(), "/user/hive/warehouse/"+tableName)
                .option(HoodieBootstrapConfig.BOOTSTRAP_KEYGEN_CLASS.key(), NonpartitionedKeyGenerator.class.getCanonicalName())
                .mode(SaveMode.Overwrite).save("/hudi/"+tableName);

        df.count();


    }
}


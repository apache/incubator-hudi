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

package org.apache.hudi.examples.java;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.bootstrap.HoodieSparkBootstrapSchemaProvider;
import org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.bootstrap.FileStatusUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
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
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.action.bootstrap.BootstrapUtils;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.avro.Schema;
import java.util.Properties;

public class HoodieSparkBootstrapWriteClientExample {
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

        if(tableTy.equals("MOR"))
            tableType = HoodieTableType.MERGE_ON_READ.name();
        else
            tableType = HoodieTableType.COPY_ON_WRITE.name();

        SparkConf sparkConf = HoodieExampleSparkUtils.defaultSparkConf("hoodie-client-example");

        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {

            // Generator of some records to be loaded in.
            HoodieExampleDataGenerator<HoodieAvroPayload> dataGen = new HoodieExampleDataGenerator<>();

            // initialize the table, if not done already
            Path path = new Path(tablePath);


            Properties properties = HoodieTableMetaClient.withPropertyBuilder()
                    .setTableName(tableName)
                    .setTableType(tableType)
                    .setPayloadClass(HoodieAvroPayload.class)
                    .setBootstrapBasePath(tablePath)
                    .setBaseFileFormat(fileFormat)
                    .setPreCombineField("sno")
                    .build();
            HoodieTableMetaClient  metaClient= HoodieTableMetaClient.initTableAndGetMetaClient(jsc.hadoopConfiguration(), tablePath, properties);



            String filePath = FileStatusUtils.toPath(BootstrapUtils.getAllLeafFoldersWithFiles(metaClient, metaClient.getFs(),
                    tablePath, new HoodieSparkEngineContext(jsc)).stream().findAny().map(p -> p.getValue().stream().findAny())
                    .orElse(null).get().getPath()).toString();

            Schema schema=null;

            TypedProperties prop = new TypedProperties();
            prop.put(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY, "sno");

            if(fileFormat.equals("orc")) {
                String structName = tableName + "_record";
                String recordNamespace = "hoodie." + tableName;
                Reader orcReader = OrcFile.createReader(new Path(filePath), OrcFile.readerOptions(jsc.hadoopConfiguration()));
                schema = AvroOrcUtils.createAvroSchemaWithDefaultValue(orcReader.getSchema(), structName, recordNamespace,true);
            }
            else {
                ParquetFileReader reader = ParquetFileReader.open(metaClient.getHadoopConf(), new Path(filePath));
                MessageType schema1 = reader.getFooter().getFileMetaData().getSchema();
                schema=  new AvroSchemaConverter().convert(schema1);
            }



            HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(tablePath)
                    .withAutoCommit(true)
                    .forTable("covid_test_orc")
                    .withSchema(schema.toString())
                    .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                            .withMaxNumDeltaCommitsBeforeCompaction(1)
                            .build())
                    .withProperties(prop)
                    .withBaseFileFormat(HoodieFileFormat.PARQUET)
                    .withBootstrapConfig(HoodieBootstrapConfig.newBuilder()
                            .withBootstrapBasePath(tablePath)
                            .withBootstrapKeyGenClass(NonpartitionedKeyGenerator.class.getCanonicalName())
                            .withFullBootstrapInputProvider(HoodieSparkBootstrapSchemaProvider.class.getName())
                            .withBootstrapParallelism(1)
                            .withBootstrapModeSelector(MetadataOnlyBootstrapModeSelector.class.getCanonicalName()).build())
                    .build();
            SparkRDDWriteClient<HoodieAvroPayload> rddClient = new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jsc), config);
            rddClient.bootstrap(Option.empty());

        }

    }

}

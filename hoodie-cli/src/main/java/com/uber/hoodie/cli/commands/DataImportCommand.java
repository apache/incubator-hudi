/*
 * Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.cli.commands;

import com.uber.hoodie.cli.HoodieCLI;
import com.uber.hoodie.cli.commands.SparkMain.SparkCommand;
import com.uber.hoodie.cli.utils.InputStreamConsumer;
import com.uber.hoodie.cli.utils.SparkUtil;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.util.Utils;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

@Component
public class DataImportCommand implements CommandMarker {

    private static Logger log = LogManager.getLogger(DataImportCommand.class);

    @CliCommand(value = "import", help = "Imports given dataset to a hoodie dataset")
    public String convert(
        @CliOption(key = "srcPath", mandatory = true, help = "Base path for the input dataset")
        final String srcPath,
        @CliOption(key = "targetPath", mandatory = true, help = "Base path for the target hoodie dataset")
        final String targetPath,
        @CliOption(key = "tableName", mandatory = true, help = "Table name")
        final String tableName,
        @CliOption(key = "tableType", mandatory = true, help = "Table type")
        final String tableType,
        @CliOption(key = "rowKeyField", mandatory = true, help = "Row key field name")
        final String rowKeyField,
        @CliOption(key = "partitionPathField", mandatory = true, help = "Partition path field name")
        final String partitionPathField,
        @CliOption(key = {"parallelism"}, mandatory = true, help = "Parallelism for hoodie insert")
        final String parallelism,
        @CliOption(key = "schemaFilePath", mandatory = true, help = "Path for Avro schema file")
        final String schemaFilePath,
        @CliOption(key = "sparkMemory", mandatory = true, help = "Spark executor memory")
        final String sparkMemory)
        throws Exception {

        boolean initialized = HoodieCLI.initConf();
        HoodieCLI.initFS(initialized);

        String sparkPropertiesPath = Utils
            .getDefaultPropertiesFile(scala.collection.JavaConversions.asScalaMap(System.getenv()));
        SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);

        sparkLauncher.addAppArgs(SparkCommand.IMPORT.toString(), srcPath, targetPath, tableName,
            tableType, rowKeyField, partitionPathField, parallelism, schemaFilePath, sparkMemory);
        Process process = sparkLauncher.launch();
        InputStreamConsumer.captureOutput(process);
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            return "Failed to import dataset to hoodie format";
        }
        return "Dataset imported to hoodie format";
    }
}

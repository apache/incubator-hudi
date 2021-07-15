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

package org.apache.hudi.schema;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * A simple schema provider, that reads off files on DFS.
 */
public class FilebasedSchemaProvider implements SchemaProviderInterface {

  private final Schema sourceSchema;

  public FilebasedSchemaProvider(Configuration conf) {
    final String sourceSchemaPath = conf.getString(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH);
    final FileSystem fs = FSUtils.getFs(sourceSchemaPath, StreamerUtil.getHadoopConf());
    try {
      this.sourceSchema = new Schema.Parser().parse(fs.open(new Path(sourceSchemaPath)));
    } catch (IOException ioe) {
      throw new HoodieIOException("Error reading schema", ioe);
    }
  }

  @Override
  public Schema getSourceSchema() {
    return sourceSchema;
  }
}

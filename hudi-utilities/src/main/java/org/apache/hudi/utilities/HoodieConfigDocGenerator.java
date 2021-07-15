/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import net.steppschuh.markdowngenerator.table.Table;
import net.steppschuh.markdowngenerator.text.heading.Heading;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.reflections.Reflections;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;

import static org.reflections.ReflectionUtils.getAllFields;
import static org.reflections.ReflectionUtils.withTypeAssignableTo;

/**
 * 1. Get all subclasses of {@link HoodieConfig}
 * 2. For each subclass, get all fields of type {@link ConfigProperty}.
 * 3. Add config key and description to table row.
 */
public class HoodieConfigDocGenerator {

  private static final Logger LOG = LogManager.getLogger(HoodieConfigDocGenerator.class);

  public static void main(String[] args) {
    Reflections reflections = new Reflections("org.apache.hudi");
    Set<Class<? extends HoodieConfig>> subTypes = reflections.getSubTypesOf(HoodieConfig.class);
    // Top heading
    StringBuilder configDocBuilder = new StringBuilder()
        .append(new Heading("Configurations", 1))
        .append("\n\n");
    for (Class<? extends HoodieConfig> subType : subTypes) {
      // sub-heading
      configDocBuilder.append(new Heading(subType.getSimpleName(), 2)).append("\n\n");
      // new table
      Table.Builder tableBuilder = new Table.Builder()
          .withAlignments(Table.ALIGN_LEFT, Table.ALIGN_LEFT, Table.ALIGN_LEFT, Table.ALIGN_LEFT, Table.ALIGN_LEFT)
          .addRow("Option Name", "Property", "Required", "Default", "Remarks");
      Set<Field> fields = getAllFields(subType, withTypeAssignableTo(ConfigProperty.class));
      for (Field field : fields) {
        ConfigProperty obj = null;
        try {
          ConfigProperty f = (ConfigProperty) field.get(obj);
          tableBuilder.addRow(field.getName(), f.key(), "NO", f.hasDefaultValue() ? f.defaultValue() : "", f.doc());
        } catch (IllegalAccessException e) {
          LOG.error("Error while getting field through reflection ", e);
        }
      }
      configDocBuilder.append(tableBuilder.build()).append("\n\n");
    }
    try {
      LOG.info("Generating markdown file");
      Files.write(Paths.get("confid_doc.md"), configDocBuilder.toString().getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      LOG.error("Error while writing to markdown file ", e);
    }
  }
}

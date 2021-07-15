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

package org.apache.hudi.common.table;

import org.apache.hudi.common.bootstrap.index.HFileBootstrapIndex;
import org.apache.hudi.common.bootstrap.index.NoOpBootstrapIndex;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Configurations on the Hoodie Table like type of ingestion, storage formats, hive table name etc Configurations are loaded from hoodie.properties, these properties are usually set during
 * initializing a path as hoodie base path and never changes during the lifetime of a hoodie table.
 *
 * @see HoodieTableMetaClient
 * @since 0.3.0
 */
public class HoodieTableConfig extends HoodieConfig implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieTableConfig.class);

  public static final String HOODIE_PROPERTIES_FILE = "hoodie.properties";

  public static final ConfigProperty<String> HOODIE_TABLE_NAME_PROP = ConfigProperty
      .key("hoodie.table.name")
      .noDefaultValue()
      .withDocumentation("Table name that will be used for registering with Hive. Needs to be same across runs.");

  public static final ConfigProperty<HoodieTableType> HOODIE_TABLE_TYPE_PROP = ConfigProperty
      .key("hoodie.table.type")
      .defaultValue(HoodieTableType.COPY_ON_WRITE)
      .withDocumentation("The table type for the underlying data, for this write. This can’t change between writes.");

  public static final ConfigProperty<HoodieTableVersion> HOODIE_TABLE_VERSION_PROP = ConfigProperty
      .key("hoodie.table.version")
      .defaultValue(HoodieTableVersion.ZERO)
      .withDocumentation("Version of table, used for running upgrade/downgrade steps between releases with potentially "
          + "breaking/backwards compatible changes.");

  public static final ConfigProperty<String> HOODIE_TABLE_PRECOMBINE_FIELD_PROP = ConfigProperty
      .key("hoodie.table.precombine.field")
      .noDefaultValue()
      .withDocumentation("Field used in preCombining before actual write. By default, when two records have the same key value, "
          + "the largest value for the precombine field determined by Object.compareTo(..), is picked.");

  public static final ConfigProperty<String> HOODIE_TABLE_PARTITION_COLUMNS_PROP = ConfigProperty
      .key("hoodie.table.partition.columns")
      .noDefaultValue()
      .withDocumentation("Columns used to partition the table. Concatenated values of these fields are used as "
          + "the partition path, by invoking toString()");

  public static final ConfigProperty<String> HOODIE_TABLE_RECORDKEY_FIELDS = ConfigProperty
      .key("hoodie.table.recordkey.fields")
      .noDefaultValue()
      .withDocumentation("Columns used to uniquely identify the table. Concatenated values of these fields are used as "
          + " the record key component of HoodieKey.");

  public static final ConfigProperty<String> HOODIE_TABLE_CREATE_SCHEMA = ConfigProperty
      .key("hoodie.table.create.schema")
      .noDefaultValue()
      .withDocumentation("Schema used when creating the table, for the first time.");

  public static final ConfigProperty<HoodieFileFormat> HOODIE_BASE_FILE_FORMAT_PROP = ConfigProperty
      .key("hoodie.table.base.file.format")
      .defaultValue(HoodieFileFormat.PARQUET)
      .withAlternatives("hoodie.table.ro.file.format")
      .withDocumentation("Base file format to store all the base file data.");

  public static final ConfigProperty<HoodieFileFormat> HOODIE_LOG_FILE_FORMAT_PROP = ConfigProperty
      .key("hoodie.table.log.file.format")
      .defaultValue(HoodieFileFormat.HOODIE_LOG)
      .withAlternatives("hoodie.table.rt.file.format")
      .withDocumentation("Log format used for the delta logs.");

  public static final ConfigProperty<String> HOODIE_TIMELINE_LAYOUT_VERSION_PROP = ConfigProperty
      .key("hoodie.timeline.layout.version")
      .noDefaultValue()
      .withDocumentation("Version of timeline used, by the table.");

  public static final ConfigProperty<String> HOODIE_PAYLOAD_CLASS_PROP = ConfigProperty
      .key("hoodie.compaction.payload.class")
      .defaultValue(OverwriteWithLatestAvroPayload.class.getName())
      .withDocumentation("Payload class to use for performing compactions, i.e merge delta logs with current base file and then "
          + " produce a new base file.");

  public static final ConfigProperty<String> HOODIE_ARCHIVELOG_FOLDER_PROP = ConfigProperty
      .key("hoodie.archivelog.folder")
      .defaultValue("archived")
      .withDocumentation("path under the meta folder, to store archived timeline instants at.");

  public static final ConfigProperty<String> HOODIE_BOOTSTRAP_INDEX_ENABLE_PROP = ConfigProperty
      .key("hoodie.bootstrap.index.enable")
      .noDefaultValue()
      .withDocumentation("Whether or not, this is a bootstrapped table, with bootstrap base data and an mapping index defined.");

  public static final ConfigProperty<String> HOODIE_BOOTSTRAP_INDEX_CLASS_PROP = ConfigProperty
      .key("hoodie.bootstrap.index.class")
      .defaultValue(HFileBootstrapIndex.class.getName())
      .withDocumentation("Implementation to use, for mapping base files to bootstrap base file, that contain actual data.");

  public static final ConfigProperty<String> HOODIE_BOOTSTRAP_BASE_PATH_PROP = ConfigProperty
      .key("hoodie.bootstrap.base.path")
      .noDefaultValue()
      .withDocumentation("Base path of the dataset that needs to be bootstrapped as a Hudi table");

  public static final String NO_OP_BOOTSTRAP_INDEX_CLASS = NoOpBootstrapIndex.class.getName();

  public HoodieTableConfig(FileSystem fs, String metaPath, String payloadClassName) {
    super();
    Path propertyPath = new Path(metaPath, HOODIE_PROPERTIES_FILE);
    LOG.info("Loading table properties from " + propertyPath);
    try {
      try (FSDataInputStream inputStream = fs.open(propertyPath)) {
        props.load(inputStream);
      }
      if (contains(HOODIE_PAYLOAD_CLASS_PROP) && payloadClassName != null
          && !getString(HOODIE_PAYLOAD_CLASS_PROP).equals(payloadClassName)) {
        setValue(HOODIE_PAYLOAD_CLASS_PROP, payloadClassName);
        try (FSDataOutputStream outputStream = fs.create(propertyPath)) {
          props.store(outputStream, "Properties saved on " + new Date(System.currentTimeMillis()));
        }
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not load Hoodie properties from " + propertyPath, e);
    }
    ValidationUtils.checkArgument(contains(HOODIE_TABLE_TYPE_PROP) && contains(HOODIE_TABLE_NAME_PROP),
        "hoodie.properties file seems invalid. Please check for left over `.updated` files if any, manually copy it to hoodie.properties and retry");
  }

  /**
   * For serializing and de-serializing.
   *
   */
  public HoodieTableConfig() {
    super();
  }

  /**
   * Initialize the hoodie meta directory and any necessary files inside the meta (including the hoodie.properties).
   */
  public static void createHoodieProperties(FileSystem fs, Path metadataFolder, Properties properties)
      throws IOException {
    if (!fs.exists(metadataFolder)) {
      fs.mkdirs(metadataFolder);
    }
    HoodieConfig hoodieConfig = new HoodieConfig(properties);
    Path propertyPath = new Path(metadataFolder, HOODIE_PROPERTIES_FILE);
    try (FSDataOutputStream outputStream = fs.create(propertyPath)) {
      if (!hoodieConfig.contains(HOODIE_TABLE_NAME_PROP)) {
        throw new IllegalArgumentException(HOODIE_TABLE_NAME_PROP.key() + " property needs to be specified");
      }
      hoodieConfig.setDefaultValue(HOODIE_TABLE_TYPE_PROP);
      if (hoodieConfig.getString(HOODIE_TABLE_TYPE_PROP).equals(HoodieTableType.MERGE_ON_READ.name())) {
        hoodieConfig.setDefaultValue(HOODIE_PAYLOAD_CLASS_PROP);
      }
      hoodieConfig.setDefaultValue(HOODIE_ARCHIVELOG_FOLDER_PROP);
      if (!hoodieConfig.contains(HOODIE_TIMELINE_LAYOUT_VERSION_PROP)) {
        // Use latest Version as default unless forced by client
        hoodieConfig.setValue(HOODIE_TIMELINE_LAYOUT_VERSION_PROP, TimelineLayoutVersion.CURR_VERSION.toString());
      }
      if (hoodieConfig.contains(HOODIE_BOOTSTRAP_BASE_PATH_PROP)) {
        // Use the default bootstrap index class.
        hoodieConfig.setDefaultValue(HOODIE_BOOTSTRAP_INDEX_CLASS_PROP, getDefaultBootstrapIndexClass(properties));
      }
      hoodieConfig.getProps().store(outputStream, "Properties saved on " + new Date(System.currentTimeMillis()));
    }
  }

  /**
   * Read the table type from the table properties and if not found, return the default.
   */
  public HoodieTableType getTableType() {
    return HoodieTableType.valueOf(getStringOrDefault(HOODIE_TABLE_TYPE_PROP));
  }

  public Option<TimelineLayoutVersion> getTimelineLayoutVersion() {
    return contains(HOODIE_TIMELINE_LAYOUT_VERSION_PROP)
        ? Option.of(new TimelineLayoutVersion(getInt(HOODIE_TIMELINE_LAYOUT_VERSION_PROP)))
        : Option.empty();
  }

  /**
   * @return the hoodie.table.version from hoodie.properties file.
   */
  public HoodieTableVersion getTableVersion() {
    return contains(HOODIE_TABLE_VERSION_PROP)
        ? HoodieTableVersion.versionFromCode(getInt(HOODIE_TABLE_VERSION_PROP))
        : HOODIE_TABLE_VERSION_PROP.defaultValue();
  }

  public void setTableVersion(HoodieTableVersion tableVersion) {
    setValue(HOODIE_TABLE_VERSION_PROP, Integer.toString(tableVersion.versionCode()));
  }

  /**
   * Read the payload class for HoodieRecords from the table properties.
   */
  public String getPayloadClass() {
    // There could be tables written with payload class from com.uber.hoodie. Need to transparently
    // change to org.apache.hudi
    return getStringOrDefault(HOODIE_PAYLOAD_CLASS_PROP).replace("com.uber.hoodie",
        "org.apache.hudi");
  }

  public String getPreCombineField() {
    return getString(HOODIE_TABLE_PRECOMBINE_FIELD_PROP);
  }

  public Option<String[]> getPartitionColumns() {
    if (contains(HOODIE_TABLE_PARTITION_COLUMNS_PROP)) {
      return Option.of(Arrays.stream(getString(HOODIE_TABLE_PARTITION_COLUMNS_PROP).split(","))
        .filter(p -> p.length() > 0).collect(Collectors.toList()).toArray(new String[]{}));
    }
    return Option.empty();
  }

  /**
   * Read the payload class for HoodieRecords from the table properties.
   */
  public String getBootstrapIndexClass() {
    // There could be tables written with payload class from com.uber.hoodie. Need to transparently
    // change to org.apache.hudi
    return getStringOrDefault(HOODIE_BOOTSTRAP_INDEX_CLASS_PROP, getDefaultBootstrapIndexClass(props));
  }

  public static String getDefaultBootstrapIndexClass(Properties props) {
    String defaultClass = HOODIE_BOOTSTRAP_INDEX_CLASS_PROP.defaultValue();
    if ("false".equalsIgnoreCase(props.getProperty(HOODIE_BOOTSTRAP_INDEX_ENABLE_PROP.key()))) {
      defaultClass = NO_OP_BOOTSTRAP_INDEX_CLASS;
    }
    return defaultClass;
  }

  public Option<String> getBootstrapBasePath() {
    return Option.ofNullable(getString(HOODIE_BOOTSTRAP_BASE_PATH_PROP));
  }

  public Option<Schema> getTableCreateSchema() {
    if (contains(HOODIE_TABLE_CREATE_SCHEMA)) {
      return Option.of(new Schema.Parser().parse(getString(HOODIE_TABLE_CREATE_SCHEMA)));
    } else {
      return Option.empty();
    }
  }

  /**
   * Read the table name.
   */
  public String getTableName() {
    return getString(HOODIE_TABLE_NAME_PROP);
  }

  /**
   * Get the base file storage format.
   *
   * @return HoodieFileFormat for the base file Storage format
   */
  public HoodieFileFormat getBaseFileFormat() {
    return HoodieFileFormat.valueOf(getStringOrDefault(HOODIE_BASE_FILE_FORMAT_PROP));
  }

  /**
   * Get the log Storage Format.
   *
   * @return HoodieFileFormat for the log Storage format
   */
  public HoodieFileFormat getLogFileFormat() {
    return HoodieFileFormat.valueOf(getStringOrDefault(HOODIE_LOG_FILE_FORMAT_PROP));
  }

  /**
   * Get the relative path of archive log folder under metafolder, for this table.
   */
  public String getArchivelogFolder() {
    return getStringOrDefault(HOODIE_ARCHIVELOG_FOLDER_PROP);
  }

  public Map<String, String> propsMap() {
    return props.entrySet().stream()
        .collect(Collectors.toMap(e -> String.valueOf(e.getKey()), e -> String.valueOf(e.getValue())));
  }
}
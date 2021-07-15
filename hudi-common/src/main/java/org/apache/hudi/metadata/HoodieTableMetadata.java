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

package org.apache.hudi.metadata;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Interface that supports querying various pieces of metadata about a hudi table.
 */
public interface HoodieTableMetadata extends Serializable, AutoCloseable {

  // Table name suffix
  String METADATA_TABLE_NAME_SUFFIX = "_metadata";
  /**
   * Timestamp for a commit when the base dataset had not had any commits yet. this is < than even
   * {@link org.apache.hudi.common.table.timeline.HoodieTimeline#INIT_INSTANT_TS}, such that the metadata table
   * can be prepped even before bootstrap is done.
   */
  String SOLO_COMMIT_TIMESTAMP = "00000000000000";
  // Key for the record which saves list of all partitions
  String RECORDKEY_PARTITION_LIST = "__all_partitions__";
  // The partition name used for non-partitioned tables
  String NON_PARTITIONED_NAME = ".";

  // Base path of the Metadata Table relative to the dataset (.hoodie/metadata)
  static final String METADATA_TABLE_REL_PATH = HoodieTableMetaClient.METAFOLDER_NAME + Path.SEPARATOR + "metadata";

  /**
   * Return the base path of the Metadata Table.
   *
   * @param tableBasePath The base path of the dataset
   */
  static String getMetadataTableBasePath(String tableBasePath) {
    return tableBasePath + Path.SEPARATOR + METADATA_TABLE_REL_PATH;
  }

  /**
   * Returns {@code True} if the given path contains a metadata table.
   *
   * @param basePath The base path to check
   */
  static boolean isMetadataTable(String basePath) {
    return basePath.endsWith(METADATA_TABLE_REL_PATH);
  }

  static HoodieTableMetadata create(HoodieEngineContext engineContext, HoodieMetadataConfig metadataConfig, String datasetBasePath,
                                    String spillableMapPath) {
    return create(engineContext, metadataConfig, datasetBasePath, spillableMapPath, false);
  }

  static HoodieTableMetadata create(HoodieEngineContext engineContext, HoodieMetadataConfig metadataConfig, String datasetBasePath,
                                    String spillableMapPath, boolean reuse) {
    if (metadataConfig.useFileListingMetadata()) {
      return new HoodieBackedTableMetadata(engineContext, metadataConfig, datasetBasePath, spillableMapPath, reuse);
    } else {
      return new FileSystemBackedTableMetadata(engineContext, new SerializableConfiguration(engineContext.getHadoopConf()),
          datasetBasePath, metadataConfig.shouldAssumeDatePartitioning());
    }
  }

  /**
   * Fetch all the files at the given partition path, per the latest snapshot of the metadata.
   */
  FileStatus[] getAllFilesInPartition(Path partitionPath) throws IOException;

  /**
   * Fetch list of all partition paths, per the latest snapshot of the metadata.
   */
  List<String> getAllPartitionPaths() throws IOException;

  /**
   * Get the instant time to which the metadata is synced w.r.t data timeline.
   */
  Option<String> getSyncedInstantTime();

  boolean isInSync();
}

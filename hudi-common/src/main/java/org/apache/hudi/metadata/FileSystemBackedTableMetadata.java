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

import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class FileSystemBackedTableMetadata implements HoodieTableMetadata {

  private static final int DEFAULT_LISTING_PARALLELISM = 1500;

  private final transient HoodieEngineContext engineContext;
  private final SerializableConfiguration hadoopConf;
  private final String datasetBasePath;
  private final boolean assumeDatePartitioning;

  public FileSystemBackedTableMetadata(HoodieEngineContext engineContext, SerializableConfiguration conf, String datasetBasePath,
                                       boolean assumeDatePartitioning) {
    this.engineContext = engineContext;
    this.hadoopConf = conf;
    this.datasetBasePath = datasetBasePath;
    this.assumeDatePartitioning = assumeDatePartitioning;
  }

  @Override
  public FileStatus[] getAllFilesInPartition(Path partitionPath) throws IOException {
    FileSystem fs = partitionPath.getFileSystem(hadoopConf.get());
    return FSUtils.getAllDataFilesInPartition(fs, partitionPath);
  }

  @Override
  public List<String> getAllPartitionPaths() throws IOException {
    if (assumeDatePartitioning) {
      FileSystem fs = new Path(datasetBasePath).getFileSystem(hadoopConf.get());
      return FSUtils.getAllPartitionFoldersThreeLevelsDown(fs, datasetBasePath);
    }

    List<Path> pathsToList = new LinkedList<>();
    pathsToList.add(new Path(datasetBasePath));
    List<String> partitionPaths = new ArrayList<>();

    while (!pathsToList.isEmpty()) {
      // TODO: Get the parallelism from HoodieWriteConfig
      int listingParallelism = Math.min(DEFAULT_LISTING_PARALLELISM, pathsToList.size());

      // List all directories in parallel
      List<Pair<Path, FileStatus[]>> dirToFileListing = engineContext.map(pathsToList, path -> {
        FileSystem fileSystem = path.getFileSystem(hadoopConf.get());
        return Pair.of(path, fileSystem.listStatus(path));
      }, listingParallelism);
      pathsToList.clear();

      // If the listing reveals a directory, add it to queue. If the listing reveals a hoodie partition, add it to
      // the results.
      dirToFileListing.forEach(p -> {
        Option<FileStatus> partitionMetaFile = Option.fromJavaOptional(Arrays.stream(p.getRight()).parallel()
            .filter(fs -> fs.getPath().getName().equals(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE))
            .findFirst());

        if (partitionMetaFile.isPresent()) {
          // Is a partition.
          String partitionName = FSUtils.getRelativePartitionPath(new Path(datasetBasePath), p.getLeft());
          partitionPaths.add(partitionName);
        } else {
          // Add sub-dirs to the queue
          pathsToList.addAll(Arrays.stream(p.getRight())
              .filter(fs -> fs.isDirectory() && !fs.getPath().getName().equals(HoodieTableMetaClient.METAFOLDER_NAME))
              .map(fs -> fs.getPath())
              .collect(Collectors.toList()));
        }
      });
    }
    return partitionPaths;
  }

  @Override
  public Option<String> getSyncedInstantTime() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Option<String> getSyncedInstantTimeForReader() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isInSync() {
    return true;
  }

  @Override
  public void close() throws Exception {
    // no-op
  }
}

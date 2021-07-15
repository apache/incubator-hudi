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

package org.apache.hudi.common.model;

import org.apache.hudi.avro.model.HoodieClusteringGroup;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Encapsulates all the needed information about a clustering group. This is needed because spark serialization
 * does not work with avro objects.
 */
public class ClusteringGroupInfo implements Serializable {

  private List<ClusteringOperation> operations;
  private int numOutputGroups;

  public static ClusteringGroupInfo create(HoodieClusteringGroup clusteringGroup) {
    List<ClusteringOperation> operations = clusteringGroup.getSlices().stream()
        .map(ClusteringOperation::create).collect(Collectors.toList());
    
    return new ClusteringGroupInfo(operations, clusteringGroup.getNumOutputFileGroups());
  }
  
  // Only for serialization/de-serialization
  @Deprecated
  public ClusteringGroupInfo() {}
  
  private ClusteringGroupInfo(final List<ClusteringOperation> operations, final int numOutputGroups) {
    this.operations = operations;
    this.numOutputGroups = numOutputGroups;
  }

  public List<ClusteringOperation> getOperations() {
    return this.operations;
  }

  public void setOperations(final List<ClusteringOperation> operations) {
    this.operations = operations;
  }

  public int getNumOutputGroups() {
    return this.numOutputGroups;
  }

  public void setNumOutputGroups(final int numOutputGroups) {
    this.numOutputGroups = numOutputGroups;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ClusteringGroupInfo that = (ClusteringGroupInfo) o;
    return Objects.equals(getFilePathsInGroup(), that.getFilePathsInGroup());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getFilePathsInGroup());
  }
  
  private String getFilePathsInGroup() {
    return getOperations().stream().map(op -> op.getDataFilePath()).collect(Collectors.joining(","));
  }
}

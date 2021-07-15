/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.callback.common;

import org.apache.hudi.common.model.HoodieWriteStat;

import java.io.Serializable;
import java.util.List;

/**
 * Base callback message, which contains commitTime and tableName only for now.
 */
public class HoodieWriteCommitCallbackMessage implements Serializable {

  private static final long serialVersionUID = -3033643980627719561L;

  /**
   * CommitTime for one batch write, this is required.
   */
  private String commitTime;

  /**
   * Table name this batch commit to.
   */
  private String tableName;

  /**
   * BathPath the table located.
   */
  private String basePath;

  /**
   * HoodieWriteStat.
   */
  private List<HoodieWriteStat> hoodieWriteStat;

  public HoodieWriteCommitCallbackMessage() {
  }

  public HoodieWriteCommitCallbackMessage(String commitTime, String tableName, String basePath, List<HoodieWriteStat> hoodieWriteStat) {
    this.commitTime = commitTime;
    this.tableName = tableName;
    this.basePath = basePath;
    this.hoodieWriteStat = hoodieWriteStat;
  }

  public String getCommitTime() {
    return commitTime;
  }

  public void setCommitTime(String commitTime) {
    this.commitTime = commitTime;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getBasePath() {
    return basePath;
  }

  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }

  public List<HoodieWriteStat> getHoodieWriteStat() {
    return hoodieWriteStat;
  }

  public void setHoodieWriteStat(List<HoodieWriteStat> hoodieWriteStat) {
    this.hoodieWriteStat = hoodieWriteStat;
  }
}

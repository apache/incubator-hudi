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

package org.apache.hudi.callback.impl;

import org.apache.hudi.callback.HoodieWriteCommitCallback;
import org.apache.hudi.callback.client.http.HoodieWriteCommitHttpCallbackClient;
import org.apache.hudi.callback.common.HoodieWriteCommitCallbackMessage;
import org.apache.hudi.callback.util.HoodieWriteCommitCallbackUtil;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * A http implementation of {@link HoodieWriteCommitCallback}.
 */
public class HoodieWriteCommitHttpCallback implements HoodieWriteCommitCallback {

  private static final Logger LOG = LogManager.getLogger(HoodieWriteCommitHttpCallback.class);

  private final HoodieWriteCommitHttpCallbackClient client;

  public HoodieWriteCommitHttpCallback(HoodieWriteConfig config) {
    this.client = new HoodieWriteCommitHttpCallbackClient(config);
  }

  @Override
  public void call(HoodieWriteCommitCallbackMessage callbackMessage) {
    // convert to json
    String callbackMsg = HoodieWriteCommitCallbackUtil.convertToJsonString(callbackMessage);
    client.send(callbackMsg);
  }
}

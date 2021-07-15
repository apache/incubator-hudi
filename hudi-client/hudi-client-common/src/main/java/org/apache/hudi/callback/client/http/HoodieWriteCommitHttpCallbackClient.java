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

package org.apache.hudi.callback.client.http;

import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.hudi.config.HoodieWriteCommitCallbackConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;

/**
 * Write commit callback http client.
 */
public class HoodieWriteCommitHttpCallbackClient implements Closeable {

  private static final Logger LOG = LogManager.getLogger(HoodieWriteCommitHttpCallbackClient.class);

  public static final String HEADER_KEY_API_KEY = "HUDI-CALLBACK-KEY";

  private final String apiKey;
  private final String url;
  private final CloseableHttpClient client;
  private HoodieWriteConfig writeConfig;

  public HoodieWriteCommitHttpCallbackClient(HoodieWriteConfig config) {
    this.writeConfig = config;
    this.apiKey = getApiKey();
    this.url = getUrl();
    this.client = getClient();
  }

  public HoodieWriteCommitHttpCallbackClient(String apiKey, String url, CloseableHttpClient client) {
    this.apiKey = apiKey;
    this.url = url;
    this.client = client;
  }

  public void send(String callbackMsg) {
    HttpPost request = new HttpPost(url);
    request.setHeader(HEADER_KEY_API_KEY, apiKey);
    request.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
    request.setEntity(new StringEntity(callbackMsg, ContentType.APPLICATION_JSON));
    try (CloseableHttpResponse response = client.execute(request)) {
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode >= 300) {
        LOG.warn(String.format("Failed to send callback message. Response was %s", response));
      } else {
        LOG.info(String.format("Sent Callback data to %s successfully !", url));
      }
    } catch (IOException e) {
      LOG.warn("Failed to send callback.", e);
    }
  }

  private String getApiKey() {
    return writeConfig.getString(HoodieWriteCommitCallbackConfig.CALLBACK_HTTP_API_KEY);
  }

  private String getUrl() {
    return writeConfig.getString(HoodieWriteCommitCallbackConfig.CALLBACK_HTTP_URL_PROP);
  }

  private CloseableHttpClient getClient() {
    int timeoutSeconds = getHttpTimeoutSeconds() * 1000;
    return HttpClientBuilder.create()
        .setDefaultRequestConfig(RequestConfig.custom()
            .setConnectTimeout(timeoutSeconds)
            .setConnectionRequestTimeout(timeoutSeconds)
            .setSocketTimeout(timeoutSeconds).build())
        .build();
  }

  private Integer getHttpTimeoutSeconds() {
    return writeConfig.getInt(HoodieWriteCommitCallbackConfig.CALLBACK_HTTP_TIMEOUT_SECONDS);
  }

  @Override
  public void close() throws IOException {
    client.close();
  }
}

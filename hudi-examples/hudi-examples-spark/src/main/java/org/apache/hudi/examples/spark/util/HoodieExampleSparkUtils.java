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

package org.apache.hudi.examples.spark.util;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

/**
 * Bunch of util methods.
 */
public class HoodieExampleSparkUtils {

  private static Map<String, String> defaultConf() {
    Map<String, String> additionalConfigs = new HashMap<>();
    additionalConfigs.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    additionalConfigs.put("spark.kryoserializer.buffer.max", "512m");
    return additionalConfigs;
  }

  public static SparkConf defaultSparkConf(String appName) {
    return buildSparkConf(appName, defaultConf());
  }

  public static SparkConf buildSparkConf(String appName, Map<String, String> additionalConfigs) {

    SparkConf sparkConf = new SparkConf().setAppName(appName);
    additionalConfigs.forEach(sparkConf::set);
    return sparkConf;
  }

  public static SparkSession defaultSparkSession(String appName) {
    return buildSparkSession(appName, defaultConf());
  }

  public static SparkSession buildSparkSession(String appName, Map<String, String> additionalConfigs) {

    SparkSession.Builder builder = SparkSession.builder().appName(appName);
    additionalConfigs.forEach(builder::config);
    return builder.getOrCreate();
  }
}

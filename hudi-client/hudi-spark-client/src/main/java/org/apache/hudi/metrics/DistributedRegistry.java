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

package org.apache.hudi.metrics;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hudi.common.metrics.Registry;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;

/**
 * Lightweight Metrics Registry to track Hudi events.
 */
public class DistributedRegistry extends AccumulatorV2<Map<String, Long>, Map<String, Long>>
    implements Registry, Serializable {
  private String name;
  ConcurrentHashMap<String, Long> counters = new ConcurrentHashMap<>();

  public DistributedRegistry(String name) {
    this.name = name;
  }

  public void register(JavaSparkContext jsc) {
    if (!isRegistered()) {
      jsc.sc().register(this);
    }
  }

  @Override
  public void clear() {
    counters.clear();
  }

  @Override
  public void increment(String name) {
    counters.merge(name,  1L, (oldValue, newValue) -> oldValue + newValue);
  }

  @Override
  public void add(String name, long value) {
    counters.merge(name,  value, (oldValue, newValue) -> oldValue + newValue);
  }

  @Override
  public void set(String name, long value) {
    counters.merge(name,  value, (oldValue, newValue) -> newValue);
  }

  /**
   * Get all Counter type metrics.
   */
  @Override
  public Map<String, Long> getAllCounts(boolean prefixWithRegistryName) {
    HashMap<String, Long> countersMap = new HashMap<>();
    counters.forEach((k, v) -> {
      String key = prefixWithRegistryName ? name + "." + k : k;
      countersMap.put(key, v);
    });
    return countersMap;
  }

  @Override
  public void add(Map<String, Long> arg) {
    arg.forEach((key, value) -> add(key, value));
  }

  @Override
  public AccumulatorV2<Map<String, Long>, Map<String, Long>> copy() {
    DistributedRegistry registry = new DistributedRegistry(name);
    counters.forEach((key, value) -> registry.add(key, value));
    return registry;
  }

  @Override
  public boolean isZero() {
    return counters.isEmpty();
  }

  @Override
  public void merge(AccumulatorV2<Map<String, Long>, Map<String, Long>> acc) {
    acc.value().forEach((key, value) -> add(key, value));
  }

  @Override
  public void reset() {
    counters.clear();
  }

  @Override
  public Map<String, Long> value() {
    return counters;
  }
}

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

package org.apache.hudi.common.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry that tracks metrics local to a single jvm process.
 */
public class LocalRegistry implements Registry {
  ConcurrentHashMap<String, Counter> counters = new ConcurrentHashMap<>();
  private final String name;

  public LocalRegistry(String name) {
    this.name = name;
  }

  @Override
  public void clear() {
    counters.clear();
  }

  @Override
  public void increment(String name) {
    getCounter(name).increment();
  }

  @Override
  public void add(String name, long value) {
    getCounter(name).add(value);
  }

  @Override
  public void set(String name, long value) {
    getCounter(name).set(value);
  }

  /**
   * Get all Counter type metrics.
   */
  @Override
  public Map<String, Long> getAllCounts(boolean prefixWithRegistryName) {
    HashMap<String, Long> countersMap = new HashMap<>();
    counters.forEach((k, v) -> {
      String key = prefixWithRegistryName ? name + "." + k : k;
      countersMap.put(key, v.getValue());
    });
    return countersMap;
  }

  private synchronized Counter getCounter(String name) {
    if (!counters.containsKey(name)) {
      counters.put(name, new Counter());
    }
    return counters.get(name);
  }
}

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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hudi.common.util.ReflectionUtils;


/**
 * Interface which defines a lightweight Metrics Registry to track Hudi events.
 */
public interface Registry extends Serializable {

  ConcurrentHashMap<String, Registry> REGISTRY_MAP = new ConcurrentHashMap<>();

  /**
   * Get (or create) the registry for a provided name.
   *
   * This function creates a {@code LocalRegistry}.
   *
   * @param registryName Name of the registry
   */
  static Registry getRegistry(String registryName) {
    return getRegistry(registryName, LocalRegistry.class.getName());
  }

  /**
   * Get (or create) the registry for a provided name and given class.
   *
   * @param registryName Name of the registry.
   * @param clazz The fully qualified name of the registry class to create.
   */
  static Registry getRegistry(String registryName, String clazz) {
    synchronized (Registry.class) {
      if (!REGISTRY_MAP.containsKey(registryName)) {
        Registry registry = (Registry)ReflectionUtils.loadClass(clazz, registryName);
        REGISTRY_MAP.put(registryName, registry);
      }
      return REGISTRY_MAP.get(registryName);
    }
  }

  /**
   * Get all registered metrics.
   *
   * @param flush clear all metrics after this operation.
   * @param prefixWithRegistryName prefix each metric name with the registry name.
   * @return
   */
  static Map<String, Long> getAllMetrics(boolean flush, boolean prefixWithRegistryName) {
    synchronized (Registry.class) {
      HashMap<String, Long> allMetrics = new HashMap<>();
      REGISTRY_MAP.forEach((registryName, registry) -> {
        allMetrics.putAll(registry.getAllCounts(prefixWithRegistryName));
        if (flush) {
          registry.clear();
        }
      });
      return allMetrics;
    }
  }

  /**
   * Clear all metrics.
   */
  void clear();

  /**
   * Increment the metric.
   *
   * @param name Name of the metric to increment.
   */
  void increment(String name);

  /**
   * Add value to the metric.
   *
   * @param name Name of the metric.
   * @param value The value to add to the metrics.
   */
  void add(String name, long value);

  /**
   * Set the value to the metric.
   *
   * If the metric does not exist, it is added. If the metrics already exists, its value is replaced with the
   * provided value.
   *
   * @param name Name of the metric.
   * @param value The value to set for the metrics.
   */
  void set(String name, long value);

  /**
   * Get all Counter type metrics.
   */
  default Map<String, Long> getAllCounts() {
    return getAllCounts(false);
  }

  /**
   * Get all Counter type metrics.
   *
   * @param prefixWithRegistryName If true, the names of all metrics are prefixed with name of this registry.
   */
  Map<String, Long> getAllCounts(boolean prefixWithRegistryName);
}

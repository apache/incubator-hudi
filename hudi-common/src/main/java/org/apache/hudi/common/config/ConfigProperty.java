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

package org.apache.hudi.common.config;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;

import java.io.Serializable;
import java.util.function.Function;
import java.util.Objects;

/**
 * ConfigProperty describes a configuration property. It contains the configuration
 * key, deprecated older versions of the key, and an optional default value for the configuration,
 * configuration descriptions and also the an infer mechanism to infer the configuration value
 * based on other configurations.
 *
 * @param <T> The type of the default value.
 */
public class ConfigProperty<T> implements Serializable {

  private final String key;

  private final T defaultValue;

  private final String doc;

  private final Option<String> sinceVersion;

  private final Option<String> deprecatedVersion;

  private final String[] alternatives;

  // provide the ability to infer config value based on other configs
  private final Option<Function<HoodieConfig, Option<T>>> inferFunction;

  ConfigProperty(String key, T defaultValue, String doc, Option<String> sinceVersion,
                 Option<String> deprecatedVersion, Option<Function<HoodieConfig, Option<T>>> inferFunc, String... alternatives) {
    this.key = Objects.requireNonNull(key);
    this.defaultValue = defaultValue;
    this.doc = doc;
    this.sinceVersion = sinceVersion;
    this.deprecatedVersion = deprecatedVersion;
    this.inferFunction = inferFunc;
    this.alternatives = alternatives;
  }

  public String key() {
    return key;
  }

  public T defaultValue() {
    if (defaultValue == null) {
      throw new HoodieException("There's no default value for this config");
    }
    return defaultValue;
  }

  public boolean hasDefaultValue() {
    return defaultValue != null;
  }

  Option<Function<HoodieConfig, Option<T>>> getInferFunc() {
    return inferFunction;
  }

  public String[] getAlternatives() {
    return alternatives;
  }

  public ConfigProperty<T> withDocumentation(String doc) {
    Objects.requireNonNull(doc);
    return new ConfigProperty<>(key, defaultValue, doc, sinceVersion, deprecatedVersion, inferFunction, alternatives);
  }

  public String doc() {
    return hasDoc() ? doc : StringUtils.EMPTY_STRING;
  }

  public boolean hasDoc() {
    return !StringUtils.isNullOrEmpty(doc);
  }

  public ConfigProperty<T> withAlternatives(String... alternatives) {
    Objects.requireNonNull(alternatives);
    return new ConfigProperty<>(key, defaultValue, doc, sinceVersion, deprecatedVersion, inferFunction, alternatives);
  }

  public ConfigProperty<T> sinceVersion(String sinceVersion) {
    Objects.requireNonNull(sinceVersion);
    return new ConfigProperty<>(key, defaultValue, doc, Option.of(sinceVersion), deprecatedVersion, inferFunction, alternatives);
  }

  public ConfigProperty<T> deprecatedAfter(String deprecatedVersion) {
    Objects.requireNonNull(deprecatedVersion);
    return new ConfigProperty<>(key, defaultValue, doc, sinceVersion, Option.of(deprecatedVersion), inferFunction, alternatives);
  }

  public ConfigProperty<T> withInferFunction(Function<HoodieConfig, Option<T>> inferFunction) {
    Objects.requireNonNull(inferFunction);
    return new ConfigProperty<>(key, defaultValue, doc, sinceVersion, deprecatedVersion, Option.of(inferFunction), alternatives);
  }

  /**
   * Create a OptionBuilder with key.
   *
   * @param key The key of the option
   * @return Return a OptionBuilder.
   */
  public static PropertyBuilder key(String key) {
    Objects.requireNonNull(key);
    return new PropertyBuilder(key);
  }

  @Override
  public String toString() {
    return String.format(
        "Key: '%s' , default: %s description: %s since version: %s deprecated after: %s)",
        key, defaultValue, doc, sinceVersion.isPresent() ? sinceVersion.get() : "version is not defined",
        deprecatedVersion.isPresent() ? deprecatedVersion.get() : "version is not defined");
  }

  /**
   * The PropertyBuilder is used to build the ConfigProperty.
   */
  public static final class PropertyBuilder {

    private final String key;

    PropertyBuilder(String key) {
      this.key = key;
    }

    public <T> ConfigProperty<T> defaultValue(T value) {
      Objects.requireNonNull(value);
      ConfigProperty<T> configProperty = new ConfigProperty<>(key, value, "", Option.empty(), Option.empty(), Option.empty());
      return configProperty;
    }

    public ConfigProperty<String> noDefaultValue() {
      ConfigProperty<String> configProperty = new ConfigProperty<>(key, null, "", Option.empty(),
          Option.empty(), Option.empty());
      return configProperty;
    }
  }
}
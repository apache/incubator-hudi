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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Properties;

/**
 * This class deals with {@link ConfigProperty} and provides get/set functionalities.
 */
public class HoodieConfig implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieConfig.class);

  public static HoodieConfig create(FSDataInputStream inputStream) throws IOException {
    HoodieConfig config = new HoodieConfig();
    config.props.load(inputStream);
    return config;
  }

  protected Properties props;

  public HoodieConfig() {
    this.props = new Properties();
  }

  public HoodieConfig(Properties props) {
    this.props = props;
  }

  public <T> void setValue(ConfigProperty<T> cfg, String val) {
    props.setProperty(cfg.key(), val);
  }

  public <T> void setDefaultValue(ConfigProperty<T> configProperty) {
    if (!contains(configProperty)) {
      Option<T> inferValue = Option.empty();
      if (configProperty.getInferFunc().isPresent()) {
        inferValue = configProperty.getInferFunc().get().apply(this);
      }
      props.setProperty(configProperty.key(), inferValue.isPresent() ? inferValue.get().toString() : configProperty.defaultValue().toString());
    }
  }

  public <T> void setDefaultValue(ConfigProperty<T> configProperty, T defaultVal) {
    if (!contains(configProperty)) {
      props.setProperty(configProperty.key(), defaultVal.toString());
    }
  }

  public <T> boolean contains(ConfigProperty<T> configProperty) {
    if (props.containsKey(configProperty.key())) {
      return true;
    }
    return Arrays.stream(configProperty.getAlternatives()).anyMatch(props::containsKey);
  }

  private <T> Option<Object> getRawValue(ConfigProperty<T> configProperty) {
    if (props.containsKey(configProperty.key())) {
      return Option.ofNullable(props.get(configProperty.key()));
    }
    for (String alternative : configProperty.getAlternatives()) {
      if (props.containsKey(alternative)) {
        LOG.warn(String.format("The configuration key '%s' has been deprecated "
                + "and may be removed in the future. Please use the new key '%s' instead.",
            alternative, configProperty.key()));
        return Option.ofNullable(props.get(alternative));
      }
    }
    return Option.empty();
  }

  protected void setDefaults(String configClassName) {
    Class<?> configClass = ReflectionUtils.getClass(configClassName);
    Arrays.stream(configClass.getDeclaredFields())
        .filter(f -> Modifier.isStatic(f.getModifiers()))
        .filter(f -> f.getType().isAssignableFrom(ConfigProperty.class))
        .forEach(f -> {
          try {
            ConfigProperty<?> cfgProp = (ConfigProperty<?>) f.get("null");
            if (cfgProp.hasDefaultValue()) {
              setDefaultValue(cfgProp);
            }
          } catch (IllegalAccessException e) {
            e.printStackTrace();
          }
        });
  }

  public <T> String getString(ConfigProperty<T> configProperty) {
    Option<Object> rawValue = getRawValue(configProperty);
    return rawValue.map(Object::toString).orElse(null);
  }

  public String getString(String key) {
    return props.getProperty(key);
  }

  public <T> Integer getInt(ConfigProperty<T> configProperty) {
    Option<Object> rawValue = getRawValue(configProperty);
    return rawValue.map(v -> Integer.parseInt(v.toString())).orElse(null);
  }

  public <T> Boolean getBoolean(ConfigProperty<T> configProperty) {
    Option<Object> rawValue = getRawValue(configProperty);
    return rawValue.map(v -> Boolean.parseBoolean(v.toString())).orElse(null);
  }

  public <T> boolean getBooleanOrDefault(ConfigProperty<T> configProperty) {
    Option<Object> rawValue = getRawValue(configProperty);
    return rawValue.map(v -> Boolean.parseBoolean(v.toString()))
            .orElse((Boolean) configProperty.defaultValue());
  }

  public <T> Long getLong(ConfigProperty<T> configProperty) {
    Option<Object> rawValue = getRawValue(configProperty);
    return rawValue.map(v -> Long.parseLong(v.toString())).orElse(null);
  }

  public <T> Float getFloat(ConfigProperty<T> configProperty) {
    Option<Object> rawValue = getRawValue(configProperty);
    return rawValue.map(v -> Float.parseFloat(v.toString())).orElse(null);
  }

  public <T> Double getDouble(ConfigProperty<T> configProperty) {
    Option<Object> rawValue = getRawValue(configProperty);
    return rawValue.map(v -> Double.parseDouble(v.toString())).orElse(null);
  }

  public <T> String getStringOrDefault(ConfigProperty<T> configProperty) {
    return getStringOrDefault(configProperty, configProperty.defaultValue().toString());
  }

  public <T> String getStringOrDefault(ConfigProperty<T> configProperty, String defaultVal) {
    Option<Object> rawValue = getRawValue(configProperty);
    return rawValue.map(Object::toString).orElse(defaultVal);
  }

  public Properties getProps() {
    return props;
  }

  public void setDefaultOnCondition(boolean condition, HoodieConfig config) {
    if (condition) {
      props.putAll(config.getProps());
    }
  }

  public <T> String getStringOrThrow(ConfigProperty<T> configProperty, String errorMessage) throws HoodieException {
    Option<Object> rawValue = getRawValue(configProperty);
    if (rawValue.isPresent()) {
      return rawValue.get().toString();
    } else {
      throw new HoodieException(errorMessage);
    }
  }
}

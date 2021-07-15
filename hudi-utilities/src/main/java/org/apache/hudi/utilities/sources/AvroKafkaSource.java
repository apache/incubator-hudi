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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.deser.KafkaAvroSchemaDeserializer;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.CheckpointUtils;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

/**
 * Reads avro serialized Kafka data, based on the confluent schema-registry.
 */
public class AvroKafkaSource extends AvroSource {

  private static final Logger LOG = LogManager.getLogger(AvroKafkaSource.class);
  // these are native kafka's config. do not change the config names.
  private static final String NATIVE_KAFKA_KEY_DESERIALIZER_PROP = "key.deserializer";
  private static final String NATIVE_KAFKA_VALUE_DESERIALIZER_PROP = "value.deserializer";
  // These are settings used to pass things to KafkaAvroDeserializer
  public static final String KAFKA_AVRO_VALUE_DESERIALIZER_PROPERTY_PREFIX = "hoodie.deltastreamer.source.kafka.value.deserializer.";
  public static final String KAFKA_AVRO_VALUE_DESERIALIZER_SCHEMA = KAFKA_AVRO_VALUE_DESERIALIZER_PROPERTY_PREFIX + "schema";

  private final KafkaOffsetGen offsetGen;
  private final HoodieDeltaStreamerMetrics metrics;

  public AvroKafkaSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider, HoodieDeltaStreamerMetrics metrics) {
    super(props, sparkContext, sparkSession, schemaProvider);

    props.put(NATIVE_KAFKA_KEY_DESERIALIZER_PROP, StringDeserializer.class);
    String deserializerClassName = props.getString(DataSourceWriteOptions.KAFKA_AVRO_VALUE_DESERIALIZER_CLASS().key(),
            DataSourceWriteOptions.KAFKA_AVRO_VALUE_DESERIALIZER_CLASS().defaultValue());

    try {
      props.put(NATIVE_KAFKA_VALUE_DESERIALIZER_PROP, Class.forName(deserializerClassName));
      if (deserializerClassName.equals(KafkaAvroSchemaDeserializer.class.getName())) {
        if (schemaProvider == null) {
          throw new HoodieIOException("SchemaProvider has to be set to use KafkaAvroSchemaDeserializer");
        }
        props.put(KAFKA_AVRO_VALUE_DESERIALIZER_SCHEMA, schemaProvider.getSourceSchema().toString());
      }
    } catch (ClassNotFoundException e) {
      String error = "Could not load custom avro kafka deserializer: " + deserializerClassName;
      LOG.error(error);
      throw new HoodieException(error, e);
    }

    this.metrics = metrics;
    offsetGen = new KafkaOffsetGen(props);
  }

  @Override
  protected InputBatch<JavaRDD<GenericRecord>> fetchNewData(Option<String> lastCheckpointStr, long sourceLimit) {
    OffsetRange[] offsetRanges = offsetGen.getNextOffsetRanges(lastCheckpointStr, sourceLimit, metrics);
    long totalNewMsgs = CheckpointUtils.totalNewMessages(offsetRanges);
    LOG.info("About to read " + totalNewMsgs + " from Kafka for topic :" + offsetGen.getTopicName());
    if (totalNewMsgs <= 0) {
      return new InputBatch<>(Option.empty(), CheckpointUtils.offsetsToStr(offsetRanges));
    }
    JavaRDD<GenericRecord> newDataRDD = toRDD(offsetRanges);
    return new InputBatch<>(Option.of(newDataRDD), KafkaOffsetGen.CheckpointUtils.offsetsToStr(offsetRanges));
  }

  private JavaRDD<GenericRecord> toRDD(OffsetRange[] offsetRanges) {
    return KafkaUtils.createRDD(sparkContext, offsetGen.getKafkaParams(), offsetRanges,
            LocationStrategies.PreferConsistent()).map(obj -> (GenericRecord) obj.value());
  }

  @Override
  public void onCommit(String lastCkptStr) {
    if (this.props.getBoolean(KafkaOffsetGen.Config.ENABLE_KAFKA_COMMIT_OFFSET.key(), KafkaOffsetGen.Config.ENABLE_KAFKA_COMMIT_OFFSET.defaultValue())) {
      offsetGen.commitOffsetToKafka(lastCkptStr);
    }
  }
}

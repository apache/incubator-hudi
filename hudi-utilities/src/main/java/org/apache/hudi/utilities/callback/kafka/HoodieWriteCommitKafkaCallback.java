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

package org.apache.hudi.utilities.callback.kafka;

import org.apache.hudi.callback.HoodieWriteCommitCallback;
import org.apache.hudi.callback.common.HoodieWriteCommitCallbackMessage;
import org.apache.hudi.callback.util.HoodieWriteCommitCallbackUtil;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Properties;

import static org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME;
import static org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallbackConfig.CALLBACK_KAFKA_ACKS;
import static org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallbackConfig.CALLBACK_KAFKA_BOOTSTRAP_SERVERS;
import static org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallbackConfig.CALLBACK_KAFKA_PARTITION;
import static org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallbackConfig.CALLBACK_KAFKA_RETRIES;
import static org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallbackConfig.CALLBACK_KAFKA_TOPIC;

/**
 * Kafka implementation of {@link HoodieWriteCommitCallback}.
 */
public class HoodieWriteCommitKafkaCallback implements HoodieWriteCommitCallback {

  private static final Logger LOG = LogManager.getLogger(HoodieWriteCommitKafkaCallback.class);

  private HoodieConfig hoodieConfig;
  private String bootstrapServers;
  private String topic;

  public HoodieWriteCommitKafkaCallback(HoodieWriteConfig config) {
    this.hoodieConfig = config;
    this.bootstrapServers = config.getString(CALLBACK_KAFKA_BOOTSTRAP_SERVERS);
    this.topic = config.getString(CALLBACK_KAFKA_TOPIC);
    validateKafkaConfig();
  }

  @Override
  public void call(HoodieWriteCommitCallbackMessage callbackMessage) {
    String callbackMsg = HoodieWriteCommitCallbackUtil.convertToJsonString(callbackMessage);
    try (KafkaProducer<String, String> producer = createProducer(hoodieConfig)) {
      ProducerRecord<String, String> record = buildProducerRecord(hoodieConfig, callbackMsg);
      producer.send(record);
      LOG.info("Send callback message succeed");
    } catch (Exception e) {
      LOG.error("Send kafka callback msg failed : ", e);
    }
  }

  /**
   * Method helps to create {@link KafkaProducer}. Here we set acks = all and retries = 3 by default to ensure no data
   * loss.
   *
   * @param hoodieConfig Kafka configs
   * @return A {@link KafkaProducer}
   */
  public KafkaProducer<String, String> createProducer(HoodieConfig hoodieConfig) {
    Properties kafkaProducerProps = new Properties();
    // bootstrap.servers
    kafkaProducerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // default "all" to ensure no message loss
    kafkaProducerProps.setProperty(ProducerConfig.ACKS_CONFIG, hoodieConfig
        .getString(CALLBACK_KAFKA_ACKS));
    // retries 3 times by default
    kafkaProducerProps.setProperty(ProducerConfig.RETRIES_CONFIG, hoodieConfig
        .getString(CALLBACK_KAFKA_RETRIES));
    kafkaProducerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProducerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");

    LOG.debug("Callback kafka producer init with configs: "
        + HoodieWriteCommitCallbackUtil.convertToJsonString(kafkaProducerProps));
    return new KafkaProducer<String, String>(kafkaProducerProps);
  }

  /**
   * Method helps to create a {@link ProducerRecord}. To ensure the order of the callback messages, we should guarantee
   * that the callback message of the same table will goes to the same partition. Therefore, if user does not specify
   * the partition, we can use the table name as {@link ProducerRecord} key.
   *
   * @param hoodieConfig       Kafka configs
   * @param callbackMsg Callback message
   * @return Callback {@link ProducerRecord}
   */
  private ProducerRecord<String, String> buildProducerRecord(HoodieConfig hoodieConfig, String callbackMsg) {
    String partition = hoodieConfig.getString(CALLBACK_KAFKA_PARTITION);
    if (null != partition) {
      return new ProducerRecord<String, String>(topic, Integer.valueOf(partition), hoodieConfig
          .getString(TABLE_NAME),
          callbackMsg);
    } else {
      return new ProducerRecord<String, String>(topic, hoodieConfig.getString(TABLE_NAME), callbackMsg);
    }
  }

  /**
   * Validate whether both {@code ProducerConfig.BOOTSTRAP_SERVERS_CONFIG} and kafka topic are configured.
   * Exception will be thrown if anyone of them is not configured.
   */
  private void validateKafkaConfig() {
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(bootstrapServers), String.format("Config %s can not be "
        + "null or empty", CALLBACK_KAFKA_BOOTSTRAP_SERVERS.key()));
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(topic), String.format("Config %s can not be null or empty",
        CALLBACK_KAFKA_TOPIC.key()));
  }

  /**
   * Callback class for this produce operation.
   */
  private static class ProducerSendCallback implements Callback {

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      LOG.info(String.format("message offset=%s partition=%s timestamp=%s topic=%s",
          metadata.offset(), metadata.partition(), metadata.timestamp(), metadata.topic()));
    }
  }

}

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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase.Helpers;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.kafka010.KafkaTestUtils;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests against {@link KafkaOffsetGen}.
 */
public class TestKafkaOffsetGen {

  private static String TEST_TOPIC_NAME = "hoodie_test";
  private KafkaTestUtils testUtils;
  private HoodieDeltaStreamerMetrics metrics = mock(HoodieDeltaStreamerMetrics.class);

  @BeforeEach
  public void setup() throws Exception {
    testUtils = new KafkaTestUtils();
    testUtils.setup();
  }

  @AfterEach
  public void teardown() throws Exception {
    testUtils.teardown();
  }

  private TypedProperties getConsumerConfigs(String autoOffsetReset) {
    TypedProperties props = new TypedProperties();
    props.put("auto.offset.reset", autoOffsetReset);
    props.put("hoodie.deltastreamer.source.kafka.topic", TEST_TOPIC_NAME);
    props.setProperty("bootstrap.servers", testUtils.brokerAddress());
    props.setProperty("key.deserializer", StringDeserializer.class.getName());
    props.setProperty("value.deserializer", StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    return props;
  }

  @Test
  public void testGetNextOffsetRangesFromEarliest() {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.createTopic(TEST_TOPIC_NAME, 1);
    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));

    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("earliest"));
    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 500, metrics);
    assertEquals(1, nextOffsetRanges.length);
    assertEquals(0, nextOffsetRanges[0].fromOffset());
    assertEquals(500, nextOffsetRanges[0].untilOffset());

    nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 5000, metrics);
    assertEquals(1, nextOffsetRanges.length);
    assertEquals(0, nextOffsetRanges[0].fromOffset());
    assertEquals(1000, nextOffsetRanges[0].untilOffset());
  }

  @Test
  public void testGetNextOffsetRangesFromLatest() {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.createTopic(TEST_TOPIC_NAME, 1);
    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("latest"));
    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 500, metrics);
    assertEquals(1, nextOffsetRanges.length);
    assertEquals(1000, nextOffsetRanges[0].fromOffset());
    assertEquals(1000, nextOffsetRanges[0].untilOffset());
  }

  @Test
  public void testGetNextOffsetRangesFromCheckpoint() {
    String lastCheckpointString = TEST_TOPIC_NAME + ",0:250";
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.createTopic(TEST_TOPIC_NAME, 1);
    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("latest"));

    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.of(lastCheckpointString), 500, metrics);
    assertEquals(1, nextOffsetRanges.length);
    assertEquals(250, nextOffsetRanges[0].fromOffset());
    assertEquals(750, nextOffsetRanges[0].untilOffset());
  }

  @Test
  public void testGetNextOffsetRangesFromMultiplePartitions() {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.createTopic(TEST_TOPIC_NAME, 2);
    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("earliest"));
    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 499, metrics);
    assertEquals(2, nextOffsetRanges.length);
    assertEquals(0, nextOffsetRanges[0].fromOffset());
    assertEquals(250, nextOffsetRanges[0].untilOffset());
    assertEquals(0, nextOffsetRanges[1].fromOffset());
    assertEquals(249, nextOffsetRanges[1].untilOffset());
  }

  @Test
  public void testGetNextOffsetRangesFromGroup() {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.createTopic(TEST_TOPIC_NAME, 2);
    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("group"));
    String lastCheckpointString = TEST_TOPIC_NAME + ",0:250,1:249";
    kafkaOffsetGen.commitOffsetToKafka(lastCheckpointString);
    // don't pass lastCheckpointString as we want to read from group committed offset
    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 300, metrics);
    assertEquals(250, nextOffsetRanges[0].fromOffset());
    assertEquals(400, nextOffsetRanges[0].untilOffset());
    assertEquals(249, nextOffsetRanges[1].fromOffset());
    assertEquals(399, nextOffsetRanges[1].untilOffset());

    // committed offsets are not present for the consumer group
    kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("group"));
    nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 300, metrics);
    assertEquals(500, nextOffsetRanges[0].fromOffset());
    assertEquals(500, nextOffsetRanges[0].untilOffset());
    assertEquals(500, nextOffsetRanges[1].fromOffset());
    assertEquals(500, nextOffsetRanges[1].untilOffset());
  }

  @Test
  public void testCheckTopicExists() {
    TypedProperties props = getConsumerConfigs("latest");
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(props);
    testUtils.createTopic(TEST_TOPIC_NAME, 1);
    boolean topicExists = kafkaOffsetGen.checkTopicExists(new KafkaConsumer(props));
    assertTrue(topicExists);
    props.put("hoodie.deltastreamer.source.kafka.topic", "random");
    kafkaOffsetGen = new KafkaOffsetGen(props);
    topicExists = kafkaOffsetGen.checkTopicExists(new KafkaConsumer(props));
    assertFalse(topicExists);
  }

  @Test
  public void testTopicNameNotPresentInProps() {
    assertThrows(HoodieNotSupportedException.class, () -> new KafkaOffsetGen(new TypedProperties()));
  }
}

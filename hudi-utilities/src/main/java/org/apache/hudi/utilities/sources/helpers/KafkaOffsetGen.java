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

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieDeltaStreamerException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;

import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.sources.AvroKafkaSource;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Source to read data from Kafka, incrementally.
 */
public class KafkaOffsetGen {

  private static final Logger LOG = LogManager.getLogger(KafkaOffsetGen.class);

  /**
   * kafka checkpoint Pattern.
   * Format: topic_name,partition_num:offset,partition_num:offset,....
   */
  private final Pattern pattern = Pattern.compile(".*,.*:.*");

  public static class CheckpointUtils {

    /**
     * Reconstruct checkpoint from timeline.
     */
    public static Map<TopicPartition, Long> strToOffsets(String checkpointStr) {
      Map<TopicPartition, Long> offsetMap = new HashMap<>();
      String[] splits = checkpointStr.split(",");
      String topic = splits[0];
      for (int i = 1; i < splits.length; i++) {
        String[] subSplits = splits[i].split(":");
        offsetMap.put(new TopicPartition(topic, Integer.parseInt(subSplits[0])), Long.parseLong(subSplits[1]));
      }
      return offsetMap;
    }

    /**
     * String representation of checkpoint
     * <p>
     * Format: topic1,0:offset0,1:offset1,2:offset2, .....
     */
    public static String offsetsToStr(OffsetRange[] ranges) {
      StringBuilder sb = new StringBuilder();
      // at least 1 partition will be present.
      sb.append(ranges[0].topic() + ",");
      sb.append(Arrays.stream(ranges).map(r -> String.format("%s:%d", r.partition(), r.untilOffset()))
              .collect(Collectors.joining(",")));
      return sb.toString();
    }

    /**
     * Compute the offset ranges to read from Kafka, while handling newly added partitions, skews, event limits.
     *
     * @param fromOffsetMap offsets where we left off last time
     * @param toOffsetMap offsets of where each partitions is currently at
     * @param numEvents maximum number of events to read.
     */
    public static OffsetRange[] computeOffsetRanges(Map<TopicPartition, Long> fromOffsetMap,
                                                    Map<TopicPartition, Long> toOffsetMap, long numEvents) {

      Comparator<OffsetRange> byPartition = Comparator.comparing(OffsetRange::partition);

      // Create initial offset ranges for each 'to' partition, with from = to offsets.
      OffsetRange[] ranges = new OffsetRange[toOffsetMap.size()];
      toOffsetMap.keySet().stream().map(tp -> {
        long fromOffset = fromOffsetMap.getOrDefault(tp, 0L);
        return OffsetRange.create(tp, fromOffset, fromOffset);
      }).sorted(byPartition).collect(Collectors.toList()).toArray(ranges);

      long allocedEvents = 0;
      Set<Integer> exhaustedPartitions = new HashSet<>();
      // keep going until we have events to allocate and partitions still not exhausted.
      while (allocedEvents < numEvents && exhaustedPartitions.size() < toOffsetMap.size()) {
        long remainingEvents = numEvents - allocedEvents;
        long eventsPerPartition =
                (long) Math.ceil((1.0 * remainingEvents) / (toOffsetMap.size() - exhaustedPartitions.size()));

        // Allocate the remaining events to non-exhausted partitions, in round robin fashion
        for (int i = 0; i < ranges.length; i++) {
          OffsetRange range = ranges[i];
          if (!exhaustedPartitions.contains(range.partition())) {
            long toOffsetMax = toOffsetMap.get(range.topicPartition());
            long toOffset = Math.min(toOffsetMax, range.untilOffset() + eventsPerPartition);
            if (toOffset == toOffsetMax) {
              exhaustedPartitions.add(range.partition());
            }
            allocedEvents += toOffset - range.untilOffset();
            // We need recompute toOffset if allocedEvents larger than numEvents.
            if (allocedEvents > numEvents) {
              long offsetsToAdd = Math.min(eventsPerPartition, (numEvents - allocedEvents));
              toOffset = Math.min(toOffsetMax, toOffset + offsetsToAdd);
            }
            ranges[i] = OffsetRange.create(range.topicPartition(), range.fromOffset(), toOffset);
          }
        }
      }

      return ranges;
    }

    public static long totalNewMessages(OffsetRange[] ranges) {
      return Arrays.stream(ranges).mapToLong(OffsetRange::count).sum();
    }
  }

  /**
   * Kafka reset offset strategies.
   */
  enum KafkaResetOffsetStrategies {
    LATEST, EARLIEST, GROUP
  }

  /**
   * Configs to be passed for this source. All standard Kafka consumer configs are also respected
   */
  public static class Config {

    private static final ConfigProperty<String> KAFKA_TOPIC_NAME = ConfigProperty
            .key("hoodie.deltastreamer.source.kafka.topic")
            .noDefaultValue()
            .withDocumentation("Kafka topic name.");

    public static final ConfigProperty<String> KAFKA_CHECKPOINT_TYPE = ConfigProperty
            .key("hoodie.deltastreamer.source.kafka.checkpoint.type")
            .defaultValue("string")
            .withDocumentation("Kafka chepoint type.");

    public static final ConfigProperty<Boolean> ENABLE_KAFKA_COMMIT_OFFSET = ConfigProperty
            .key("hoodie.deltastreamer.source.kafka.enable.commit.offset")
            .defaultValue(false)
            .withDocumentation("Automatically submits offset to kafka.");

    public static final ConfigProperty<Long> MAX_EVENTS_FROM_KAFKA_SOURCE_PROP = ConfigProperty
            .key("hoodie.deltastreamer.kafka.source.maxEvents")
            .defaultValue(5000000L)
            .withDocumentation("Maximum number of records obtained in each batch.");

    private static final ConfigProperty<KafkaResetOffsetStrategies> KAFKA_AUTO_OFFSET_RESET = ConfigProperty
            .key("auto.offset.reset")
            .defaultValue(KafkaResetOffsetStrategies.LATEST)
            .withDocumentation("Kafka consumer strategy for reading data.");

    public static final String KAFKA_CHECKPOINT_TYPE_TIMESTAMP = "timestamp";
  }

  private final Map<String, Object> kafkaParams;
  private final TypedProperties props;
  protected final String topicName;
  private KafkaResetOffsetStrategies autoResetValue;
  private final String kafkaCheckpointType;

  public KafkaOffsetGen(TypedProperties props) {
    this.props = props;
    kafkaParams = excludeHoodieConfigs(props);
    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(Config.KAFKA_TOPIC_NAME.key()));
    topicName = props.getString(Config.KAFKA_TOPIC_NAME.key());
    kafkaCheckpointType = props.getString(Config.KAFKA_CHECKPOINT_TYPE.key(), "string");
    String kafkaAutoResetOffsetsStr = props.getString(Config.KAFKA_AUTO_OFFSET_RESET.key(), Config.KAFKA_AUTO_OFFSET_RESET.defaultValue().name().toLowerCase());
    boolean found = false;
    for (KafkaResetOffsetStrategies entry: KafkaResetOffsetStrategies.values()) {
      if (entry.name().toLowerCase().equals(kafkaAutoResetOffsetsStr)) {
        found = true;
        autoResetValue = entry;
        break;
      }
    }
    if (!found) {
      throw new HoodieDeltaStreamerException(Config.KAFKA_AUTO_OFFSET_RESET + " config set to unknown value " + kafkaAutoResetOffsetsStr);
    }
    if (autoResetValue.equals(KafkaResetOffsetStrategies.GROUP)) {
      this.kafkaParams.put(Config.KAFKA_AUTO_OFFSET_RESET.key(), Config.KAFKA_AUTO_OFFSET_RESET.defaultValue().name().toLowerCase());
    }
  }

  public OffsetRange[] getNextOffsetRanges(Option<String> lastCheckpointStr, long sourceLimit, HoodieDeltaStreamerMetrics metrics) {

    // Obtain current metadata for the topic
    Map<TopicPartition, Long> fromOffsets;
    Map<TopicPartition, Long> toOffsets;
    try (KafkaConsumer consumer = new KafkaConsumer(kafkaParams)) {
      if (!checkTopicExists(consumer)) {
        throw new HoodieException("Kafka topic:" + topicName + " does not exist");
      }
      List<PartitionInfo> partitionInfoList;
      partitionInfoList = consumer.partitionsFor(topicName);
      Set<TopicPartition> topicPartitions = partitionInfoList.stream()
              .map(x -> new TopicPartition(x.topic(), x.partition())).collect(Collectors.toSet());

      if (Config.KAFKA_CHECKPOINT_TYPE_TIMESTAMP.equals(kafkaCheckpointType) && isValidCheckpointType(lastCheckpointStr)) {
        lastCheckpointStr = getOffsetsByTimestamp(consumer, partitionInfoList, topicPartitions, topicName, Long.parseLong(lastCheckpointStr.get()));
      }
      // Determine the offset ranges to read from
      if (lastCheckpointStr.isPresent() && !lastCheckpointStr.get().isEmpty() && checkTopicCheckpoint(lastCheckpointStr)) {
        fromOffsets = fetchValidOffsets(consumer, lastCheckpointStr, topicPartitions);
        metrics.updateDeltaStreamerKafkaDelayCountMetrics(delayOffsetCalculation(lastCheckpointStr, topicPartitions, consumer));
      } else {
        switch (autoResetValue) {
          case EARLIEST:
            fromOffsets = consumer.beginningOffsets(topicPartitions);
            break;
          case LATEST:
            fromOffsets = consumer.endOffsets(topicPartitions);
            break;
          case GROUP:
            fromOffsets = getGroupOffsets(consumer, topicPartitions);
            break;
          default:
            throw new HoodieNotSupportedException("Auto reset value must be one of 'earliest' or 'latest' or 'group' ");
        }
      }

      // Obtain the latest offsets.
      toOffsets = consumer.endOffsets(topicPartitions);
    }

    // Come up with final set of OffsetRanges to read (account for new partitions, limit number of events)
    long maxEventsToReadFromKafka = props.getLong(Config.MAX_EVENTS_FROM_KAFKA_SOURCE_PROP.key(),
            Config.MAX_EVENTS_FROM_KAFKA_SOURCE_PROP.defaultValue());

    long numEvents;
    if (sourceLimit == Long.MAX_VALUE) {
      numEvents = maxEventsToReadFromKafka;
      LOG.info("SourceLimit not configured, set numEvents to default value : " + maxEventsToReadFromKafka);
    } else {
      numEvents = sourceLimit;
    }

    if (numEvents < toOffsets.size()) {
      throw new HoodieException("sourceLimit should not be less than the number of kafka partitions");
    }

    return CheckpointUtils.computeOffsetRanges(fromOffsets, toOffsets, numEvents);
  }

  /**
   * Fetch checkpoint offsets for each partition.
   * @param consumer instance of {@link KafkaConsumer} to fetch offsets from.
   * @param lastCheckpointStr last checkpoint string.
   * @param topicPartitions set of topic partitions.
   * @return a map of Topic partitions to offsets.
   */
  private Map<TopicPartition, Long> fetchValidOffsets(KafkaConsumer consumer,
                                                        Option<String> lastCheckpointStr, Set<TopicPartition> topicPartitions) {
    Map<TopicPartition, Long> earliestOffsets = consumer.beginningOffsets(topicPartitions);
    Map<TopicPartition, Long> checkpointOffsets = CheckpointUtils.strToOffsets(lastCheckpointStr.get());
    boolean checkpointOffsetReseter = checkpointOffsets.entrySet().stream()
        .anyMatch(offset -> offset.getValue() < earliestOffsets.get(offset.getKey()));
    return checkpointOffsetReseter ? earliestOffsets : checkpointOffsets;
  }

  /**
   * Check if the checkpoint is a timestamp.
   * @param lastCheckpointStr
   * @return
   */
  private Boolean isValidCheckpointType(Option<String> lastCheckpointStr) {
    if (!lastCheckpointStr.isPresent()) {
      return false;
    }
    Pattern pattern = Pattern.compile("[-+]?[0-9]+(\\.[0-9]+)?");
    Matcher isNum = pattern.matcher(lastCheckpointStr.get());
    return isNum.matches() && (lastCheckpointStr.get().length() == 13 || lastCheckpointStr.get().length() == 10);
  }

  private Long delayOffsetCalculation(Option<String> lastCheckpointStr, Set<TopicPartition> topicPartitions, KafkaConsumer consumer) {
    Long delayCount = 0L;
    Map<TopicPartition, Long> checkpointOffsets = CheckpointUtils.strToOffsets(lastCheckpointStr.get());
    Map<TopicPartition, Long> lastOffsets = consumer.endOffsets(topicPartitions);

    for (Map.Entry<TopicPartition, Long> entry : lastOffsets.entrySet()) {
      Long offect = checkpointOffsets.getOrDefault(entry.getKey(), 0L);
      delayCount += entry.getValue() - offect > 0 ? entry.getValue() - offect : 0L;
    }
    return delayCount;
  }

  /**
   * Get the checkpoint by timestamp.
   * This method returns the checkpoint format based on the timestamp.
   * example:
   * 1. input: timestamp, etc.
   * 2. output: topicName,partition_num_0:100,partition_num_1:101,partition_num_2:102.
   *
   * @param consumer
   * @param topicName
   * @param timestamp
   * @return
   */
  private Option<String> getOffsetsByTimestamp(KafkaConsumer consumer, List<PartitionInfo> partitionInfoList, Set<TopicPartition> topicPartitions,
                                               String topicName, Long timestamp) {

    Map<TopicPartition, Long> topicPartitionsTimestamp = partitionInfoList.stream()
                                                    .map(x -> new TopicPartition(x.topic(), x.partition()))
                                                    .collect(Collectors.toMap(Function.identity(), x -> timestamp));

    Map<TopicPartition, Long> earliestOffsets = consumer.beginningOffsets(topicPartitions);
    Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestamp = consumer.offsetsForTimes(topicPartitionsTimestamp);

    StringBuilder sb = new StringBuilder();
    sb.append(topicName + ",");
    for (Map.Entry<TopicPartition, OffsetAndTimestamp> map : offsetAndTimestamp.entrySet()) {
      if (map.getValue() != null) {
        sb.append(map.getKey().partition()).append(":").append(map.getValue().offset()).append(",");
      } else {
        sb.append(map.getKey().partition()).append(":").append(earliestOffsets.get(map.getKey())).append(",");
      }
    }
    return Option.of(sb.deleteCharAt(sb.length() - 1).toString());
  }


  /**
   * Check if topic exists.
   * @param consumer kafka consumer
   * @return
   */
  public boolean checkTopicExists(KafkaConsumer consumer)  {
    Map<String, List<PartitionInfo>> result = consumer.listTopics();
    return result.containsKey(topicName);
  }

  private boolean checkTopicCheckpoint(Option<String> lastCheckpointStr) {
    Matcher matcher = pattern.matcher(lastCheckpointStr.get());
    return matcher.matches();
  }

  public String getTopicName() {
    return topicName;
  }

  public Map<String, Object> getKafkaParams() {
    return kafkaParams;
  }

  private Map<String, Object> excludeHoodieConfigs(TypedProperties props) {
    Map<String, Object> kafkaParams = new HashMap<>();
    props.keySet().stream().filter(prop -> {
      // In order to prevent printing unnecessary warn logs, here filter out the hoodie
      // configuration items before passing to kafkaParams
      return !prop.toString().startsWith("hoodie.")
              // We need to pass some properties to kafka client so that KafkaAvroSchemaDeserializer can use it
              || prop.toString().startsWith(AvroKafkaSource.KAFKA_AVRO_VALUE_DESERIALIZER_PROPERTY_PREFIX);
    }).forEach(prop -> {
      kafkaParams.put(prop.toString(), props.get(prop.toString()));
    });
    return kafkaParams;
  }

  /**
   * Commit offsets to Kafka only after hoodie commit is successful.
   * @param checkpointStr checkpoint string containing offsets.
   */
  public void commitOffsetToKafka(String checkpointStr) {
    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(ConsumerConfig.GROUP_ID_CONFIG));
    Map<TopicPartition, Long> offsetMap = CheckpointUtils.strToOffsets(checkpointStr);
    Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>(offsetMap.size());
    try (KafkaConsumer consumer = new KafkaConsumer(kafkaParams)) {
      offsetMap.forEach((topicPartition, offset) -> offsetAndMetadataMap.put(topicPartition, new OffsetAndMetadata(offset)));
      consumer.commitSync(offsetAndMetadataMap);
    } catch (CommitFailedException | TimeoutException e) {
      LOG.warn("Committing offsets to Kafka failed, this does not impact processing of records", e);
    }
  }

  private Map<TopicPartition, Long> getGroupOffsets(KafkaConsumer consumer, Set<TopicPartition> topicPartitions) {
    Map<TopicPartition, Long> fromOffsets = new HashMap<>();
    for (TopicPartition topicPartition : topicPartitions) {
      OffsetAndMetadata committedOffsetAndMetadata = consumer.committed(topicPartition);
      if (committedOffsetAndMetadata != null) {
        fromOffsets.put(topicPartition, committedOffsetAndMetadata.offset());
      } else {
        LOG.warn("There are no commits associated with this consumer group, starting to consume from latest offset");
        fromOffsets = consumer.endOffsets(topicPartitions);
        break;
      }
    }
    return fromOffsets;
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.opal.common.rpcQueue;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.pinot.opal.common.OffsetInfo;
import org.slf4j.Logger;


public abstract class KafkaQueueConsumer<K, V> implements QueueConsumer<K, V> {

  protected abstract KafkaConsumer<K, V> getConsumer();

  /**
   * Subscribe to the topic specified
   * @param topic topic to subscribe
   */
  public synchronized void subscribe(String topic) {
    getLogger().info("Trying to subscribe to kafka topic {}", topic);
    List<PartitionInfo> partitionInfos = getConsumer().partitionsFor(topic);
    Preconditions.checkState(partitionInfos != null && partitionInfos.size() > 0, "topic doesn't have any partitions");
    Set<TopicPartition> subscribedTopicPartitions = new HashSet<>(getConsumer().assignment());
    partitionInfos.forEach(pi -> subscribedTopicPartitions.add(new TopicPartition(topic, pi.partition())));
    getLogger().info("Total subscribed topic partitions count: {}", partitionInfos.size());
    getConsumer().assign(subscribedTopicPartitions);
  }

  /**
   * Subscribe to the topic and partition specified
   * @param topic topic to subscribe
   * @param partition partition to subscribe
   */
  public synchronized void subscribe(String topic, Integer partition) {
    getLogger().info("Trying to subscribe to kafka topic: {}, partition: {}", topic, partition);
    Set<TopicPartition> subscribedTopicPartitions = new HashSet<>(getConsumer().assignment());
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    if (subscribedTopicPartitions.contains(topicPartition)) {
      getLogger().error("Already subscribed to topic: {}, partition: {} previously, skipping", topic, partition);
    } else {
      subscribedTopicPartitions.add(topicPartition);
      getLogger().info("Total subscribed topic partitions count: {}", subscribedTopicPartitions.size());
      getConsumer().assign(subscribedTopicPartitions);
      getLogger().info("Successfully subscribed to topic: {}, partition: {}", topic, partition);
    }
  }

  /**
   * Unsubscribe to the topic specified
   * @param topic topic to unsubscribe
   */
  public synchronized void unsubscribe(String topic) {
    getLogger().info("Trying to unsubscribe to kafka topic {}", topic);
    Set<TopicPartition> resultTopicPartitions =
        getConsumer().assignment().stream().filter(tp -> !tp.topic().equals(topic)).collect(Collectors.toSet());
    getLogger().info("Total subscribed topic partitions count: {}", resultTopicPartitions.size());
    getConsumer().assign(resultTopicPartitions);
  }

  /**
   * Unsubscribe to the topic and partition specified
   * @param topic topic to unsubscribe
   * @param partition partition to unsubscribe
   */
  public synchronized void unsubscribe(String topic, Integer partition) {
    getLogger().info("Trying to unsubscribe to kafka topic: {}, partition: {}", topic, partition);
    Set<TopicPartition> resultTopicPartitions =
        getConsumer().assignment().stream().filter(tp -> !(tp.topic().equals(topic) && tp.partition() == partition)).collect(
            Collectors.toSet());
    getLogger().info("Total subscribed topic partitions count: {}", resultTopicPartitions.size());
    getConsumer().assign(resultTopicPartitions);
  }

  public abstract Logger getLogger();

  @Override
  public synchronized List<QueueConsumerRecord<K, V>> getRequests(long timeout, TimeUnit timeUnit) {
    ConsumerRecords<K, V> records = getConsumerRecords(timeout, timeUnit);
    List<QueueConsumerRecord<K, V>> msgList = new ArrayList<>(records.count());
    for (ConsumerRecord<K, V> record : records) {
      msgList.add(
          new QueueConsumerRecord<>(record.topic(), record.partition(), record.offset(), record.key(), record.value()));
    }
    return msgList;
  }

  private synchronized ConsumerRecords<K, V> getConsumerRecords(long timeout, TimeUnit timeUnit) {
    return getConsumer().poll(timeUnit.toMillis(timeout));
  }

  @Override
  public synchronized void ackOffset() {
    getConsumer().commitSync();
  }

  public synchronized void ackOffset(OffsetInfo offsetInfo) {
    getLogger().info("committing offset for consumer");
    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetInfo.getOffsetMap().entrySet()) {
      getLogger().info("topic {} partition {} offset {}", entry.getKey().topic(), entry.getKey().partition(),
          entry.getValue().offset());
    }
    getConsumer().commitSync(offsetInfo.getOffsetMap());
  }

  public void close() {
    getConsumer().close();
  }
}

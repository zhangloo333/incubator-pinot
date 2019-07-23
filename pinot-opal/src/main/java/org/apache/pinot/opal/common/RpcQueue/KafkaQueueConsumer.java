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
package org.apache.pinot.opal.common.RpcQueue;

import com.google.common.base.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.pinot.opal.distributed.keyCoordinator.common.OffsetInfo;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class KafkaQueueConsumer<K, V> implements QueueConsumer<K, V> {

  protected abstract KafkaConsumer<K, V> getConsumer();

  public synchronized void subscribe(Map<String, List<Integer>> topicPartitionMap) {
    Preconditions.checkState(topicPartitionMap.size() > 0, "topic partition map should not be empty");
    List<TopicPartition> subscribePartitions = new ArrayList<>();
    for (Map.Entry<String, List<Integer>> entry: topicPartitionMap.entrySet()) {
      String topic = entry.getKey();
      List<Integer> partitions = entry.getValue();
      if (partitions.size() == 0) {
        getLogger().error("topic {} has 0 partitions given for its input", topic);
      }
      getLogger().info("adding topic {} with partitions {}", topic,
          partitions.stream().map(Object::toString).collect(Collectors.joining(",")));
      for (int partition: partitions) {
        subscribePartitions.add(new TopicPartition(topic, partition));
      }
    }
    getConsumer().assign(subscribePartitions);
  }

  public synchronized void subscribe(String topic) {
    getLogger().info("subscribing to kafka topic {}", topic);
    List<PartitionInfo> partitionInfos = getConsumer().partitionsFor(topic);
    Preconditions.checkState(partitionInfos != null && partitionInfos.size() > 0,
        "topic doesn't have any partitions");
    List<TopicPartition> subscribedTopicPartitions = new ArrayList<>(getConsumer().assignment());
    partitionInfos.forEach(pi -> subscribedTopicPartitions.add(new TopicPartition(topic, pi.partition())));
    getLogger().info("total subscribed topic partitions count: {}", partitionInfos.size());
    getConsumer().assign(subscribedTopicPartitions);
  }

  public synchronized void unsubscribe(String topic) {
    getLogger().info("subscribing to kafka topic {}", topic);
    List<TopicPartition> resultTopicPartitions = getConsumer().assignment()
        .stream()
        .filter(tp -> !tp.topic().equals(topic))
        .collect(Collectors.toList());
    getLogger().info("total subscribed topic partitions count: {}", resultTopicPartitions.size());
    getConsumer().assign(resultTopicPartitions);
  }

  public abstract Logger getLogger();

  @Override
  public synchronized List<V> getRequests(long timeout, TimeUnit timeUnit) {
    ConsumerRecords<K, V> records = getConsumerRecords(timeout, timeUnit);
    List<V> msgList = new ArrayList<>(records.count());
    for (ConsumerRecord<K, V> record : records) {
      msgList.add(record.value());
    }
    return msgList;
  }

  public synchronized ConsumerRecords<K, V> getConsumerRecords(long timeout, TimeUnit timeUnit) {
    return getConsumer().poll(timeUnit.toMillis(timeout));
  }

  @Override
  public synchronized void ackOffset() {
    getConsumer().commitSync();
  }

  public synchronized void ackOffset(OffsetInfo offsetInfo) {
    getLogger().info("committing offset for consumer");
    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry: offsetInfo.getOffsetMap().entrySet())  {
      getLogger().info("topic {} partition {} offset {}", entry.getKey().topic(), entry.getKey().partition(),
          entry.getValue().offset());
    }
    getConsumer().commitSync(offsetInfo.getOffsetMap());
  }

  public void close() {
    getConsumer().close();
  }
}

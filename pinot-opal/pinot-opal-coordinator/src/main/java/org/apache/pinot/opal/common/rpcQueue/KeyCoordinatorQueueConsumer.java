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
import com.google.common.collect.ImmutableMap;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.pinot.opal.common.config.CommonConfig;
import org.apache.pinot.opal.common.CoordinatorConfig;
import org.apache.pinot.opal.common.messages.KeyCoordinatorQueueMsg;
import org.apache.pinot.opal.common.utils.CommonUtils;
import org.apache.pinot.opal.common.DistributedCommonUtils;
import org.apache.pinot.opal.keyCoordinator.starter.KeyCoordinatorConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class KeyCoordinatorQueueConsumer extends KafkaQueueConsumer<Integer, KeyCoordinatorQueueMsg> {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorQueueConsumer.class);

  private KafkaConsumer<Integer, KeyCoordinatorQueueMsg> _consumer;

  /**
   * @param conf configuration of the kafka key coordinator queue consumer
   */
  public void init(Configuration conf) {
    String _topic = conf.getString(CommonConfig.RPC_QUEUE_CONFIG.TOPIC_KEY);
    List<String> _partitions = conf.getList(KeyCoordinatorConf.KEY_COORDINATOR_PARTITIONS);
    String _consumerGroupPrefix = conf.getString(CoordinatorConfig.KAFKA_CONFIG.CONSUMER_GROUP_PREFIX_KEY, KeyCoordinatorConf.KAFKA_CONSUMER_GROUP_ID_PREFIX);
    String hostname = conf.getString(CommonConfig.RPC_QUEUE_CONFIG.HOSTNAME_KEY);
    Preconditions.checkState(StringUtils.isNotEmpty(_topic), "kafka consumer topic should not be empty");
    Preconditions.checkState(_partitions != null && _partitions.size() > 0, "kafka partitions list should not be empty");

    Properties kafkaProperties = CommonUtils.getPropertiesFromConf(conf.subset(CoordinatorConfig.KAFKA_CONFIG.KAFKA_CONFIG_KEY));
    kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
    kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, _consumerGroupPrefix + hostname);
    kafkaProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, DistributedCommonUtils.getClientId(hostname));
    kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    List<Integer> integerPartitions = _partitions.stream().map(Integer::parseInt).collect(Collectors.toList());
    _consumer = new KafkaConsumer<>(kafkaProperties);
    subscribe(ImmutableMap.of(_topic, integerPartitions));
  }

  @Override
  protected KafkaConsumer<Integer, KeyCoordinatorQueueMsg> getConsumer() {
    Preconditions.checkState(_consumer != null, "consumer is not initialized yet");
    return _consumer;
  }

  @Override
  public Logger getLogger() {
    return LOGGER;
  }
}

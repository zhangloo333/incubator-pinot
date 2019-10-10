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
package org.apache.pinot.grigio.common.rpcQueue;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration.Configuration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.pinot.grigio.common.config.CommonConfig;
import org.apache.pinot.grigio.common.CoordinatorConfig;
import org.apache.pinot.grigio.common.messages.LogCoordinatorMessage;
import org.apache.pinot.grigio.common.metrics.GrigioMetrics;
import org.apache.pinot.grigio.common.utils.CommonUtils;
import org.apache.pinot.grigio.common.DistributedCommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * consumer to ingest data from key coordinator output kafka topic, run in segment updater to fetch all update events
 */
public class SegmentUpdateQueueConsumer extends KafkaQueueConsumer<String, LogCoordinatorMessage> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentUpdateQueueConsumer.class);

  public static final String DEFAULT_CONSUMER_GROUP_ID_PREFIX = "pinot_upsert_updater_";

  private KafkaConsumer<String, LogCoordinatorMessage> _consumer;
  private GrigioMetrics _metrics;

  @Override
  public void init(Configuration conf, GrigioMetrics metrics) {
    this._metrics = metrics;
    String hostName = conf.getString(CommonConfig.RPC_QUEUE_CONFIG.HOSTNAME_KEY);
    String groupid = conf.getString(CoordinatorConfig.KAFKA_CONFIG.CONSUMER_GROUP_PREFIX_KEY,
        DEFAULT_CONSUMER_GROUP_ID_PREFIX) + hostName;

    LOGGER.info("creating segment updater kafka consumer with group id {}", groupid);
    Properties kafkaProperties = CommonUtils.getPropertiesFromConf(conf.subset(CoordinatorConfig.KAFKA_CONFIG.KAFKA_CONFIG_KEY));
    kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
    kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupid);
    kafkaProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, DistributedCommonUtils.getClientId(hostName));
    kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    _consumer = new KafkaConsumer<>(kafkaProperties);
  }

  @Override
  public void subscribeForTable(String tableName, String topicPrefix) {
    String topicName = DistributedCommonUtils.getKafkaTopicFromTableName(tableName, topicPrefix);
    LOGGER.info("subscribing for table {}, kafka topic {}", tableName, topicName);
    this.subscribe(topicName);
  }

  @Override
  public void unsubscribeForTable(String tableName, String topicPrefix) {
    String topicName = DistributedCommonUtils.getKafkaTopicFromTableName(tableName, topicPrefix);
    LOGGER.info("unsubscribing for table {}, kafka topic {}", tableName, topicName);
    this.unsubscribe(topicName);
  }

  @Override
  protected KafkaConsumer<String, LogCoordinatorMessage> getConsumer() {
    Preconditions.checkState(_consumer != null, "consumer is not initialized yet");
    return _consumer;
  }

  @Override
  protected GrigioMetrics getMetrics() {
    return _metrics;
  }

  @Override
  public Logger getLogger() {
    return LOGGER;
  }

}

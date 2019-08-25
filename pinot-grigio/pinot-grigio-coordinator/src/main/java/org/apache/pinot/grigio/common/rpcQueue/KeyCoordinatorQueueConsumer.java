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
import org.apache.pinot.grigio.common.CoordinatorConfig;
import org.apache.pinot.grigio.common.DistributedCommonUtils;
import org.apache.pinot.grigio.common.config.CommonConfig;
import org.apache.pinot.grigio.common.messages.KeyCoordinatorQueueMsg;
import org.apache.pinot.grigio.common.metrics.GrigioMetrics;
import org.apache.pinot.grigio.common.utils.CommonUtils;
import org.apache.pinot.grigio.keyCoordinator.starter.KeyCoordinatorConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


/**
 * Consumer to ingest data from key coordinator messages produced by pinot servers into key coordinators
 */
public class KeyCoordinatorQueueConsumer extends KafkaQueueConsumer<Integer, KeyCoordinatorQueueMsg> {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorQueueConsumer.class);

  private KafkaConsumer<Integer, KeyCoordinatorQueueMsg> _consumer;
  private GrigioMetrics _metrics;

  /**
   * @param conf configuration of the kafka key coordinator queue consumer
   */
  @Override
  public void init(Configuration conf, GrigioMetrics metrics) {
    this._metrics = metrics;
    String _consumerGroupPrefix = conf.getString(CoordinatorConfig.KAFKA_CONFIG.CONSUMER_GROUP_PREFIX_KEY, KeyCoordinatorConf.KAFKA_CONSUMER_GROUP_ID_PREFIX);
    String hostname = conf.getString(CommonConfig.RPC_QUEUE_CONFIG.HOSTNAME_KEY);

    Properties kafkaProperties = CommonUtils.getPropertiesFromConf(conf.subset(CoordinatorConfig.KAFKA_CONFIG.KAFKA_CONFIG_KEY));
    kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
    kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, _consumerGroupPrefix + hostname);
    kafkaProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, DistributedCommonUtils.getClientId(hostname));
    kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    _consumer = new KafkaConsumer<>(kafkaProperties);
  }

  @Override
  public void subscribeForTable(String table) {
    // nothing as key coordinator don't subscribe for table
  }

  @Override
  public void unsubscribeForTable(String table) {
    // nothing as key coordinator don't subscribe for table
  }

  @Override
  protected KafkaConsumer<Integer, KeyCoordinatorQueueMsg> getConsumer() {
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

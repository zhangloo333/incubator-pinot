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
package com.linkedin.pinot.opal.distributed.keyCoordinator.internal;

import com.linkedin.pinot.opal.common.Config.CommonConfig;
import com.linkedin.pinot.opal.common.RpcQueue.KafkaQueueProducer;
import com.linkedin.pinot.opal.common.messages.LogCoordinatorMessage;
import com.linkedin.pinot.opal.common.utils.CommonUtils;
import com.linkedin.pinot.opal.distributed.keyCoordinator.common.DistributedCommonUtils;
import com.linkedin.pinot.opal.distributed.keyCoordinator.common.IntPartitioner;
import com.linkedin.pinot.opal.distributed.keyCoordinator.serverIngestion.KeyCoordinatorQueueProducer;
import org.apache.commons.configuration.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class LogCoordinatorQueueProducer extends KafkaQueueProducer<Integer, LogCoordinatorMessage> {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorQueueProducer.class);

  private final KafkaProducer<Integer, LogCoordinatorMessage> _kafkaProducer;
  private final String _topic;

  public LogCoordinatorQueueProducer(Configuration conf) {
    final Properties kafkaProducerConfig = CommonUtils.getPropertiesFromConf(
        conf.subset(CommonConfig.KAFKA_CONFIG.KAFKA_CONFIG_KEY));
    String hostName = conf.getString(CommonConfig.KAFKA_CONFIG.HOSTNAME_KEY);
    _topic = conf.getString(CommonConfig.KAFKA_CONFIG.TOPIC_KEY);

    kafkaProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
    kafkaProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LogCoordinatorMessage.LogCoordinatorMessageSerializer.class.getName());
    kafkaProducerConfig.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, IntPartitioner.class.getName());
    DistributedCommonUtils.setKakfaLosslessProducerConfig(kafkaProducerConfig, hostName);

    this._kafkaProducer = new KafkaProducer<>(kafkaProducerConfig);
  }

  @Override
  protected KafkaProducer<Integer, LogCoordinatorMessage> getKafkaNativeProducer() {
    return _kafkaProducer;
  }

  @Override
  protected String getDefaultTopic() {
    return _topic;
  }
}

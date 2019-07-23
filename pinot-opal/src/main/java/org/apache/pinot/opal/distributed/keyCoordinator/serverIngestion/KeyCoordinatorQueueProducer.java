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
package org.apache.pinot.opal.distributed.keyCoordinator.serverIngestion;

import org.apache.commons.configuration.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.pinot.opal.common.Config.CommonConfig;
import org.apache.pinot.opal.common.RpcQueue.KafkaQueueProducer;
import org.apache.pinot.opal.common.messages.KeyCoordinatorQueueMsg;
import org.apache.pinot.opal.common.utils.CommonUtils;
import org.apache.pinot.opal.distributed.keyCoordinator.common.DistributedCommonUtils;
import org.apache.pinot.opal.distributed.keyCoordinator.common.IntPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KeyCoordinatorQueueProducer extends KafkaQueueProducer<Integer, KeyCoordinatorQueueMsg> {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorQueueProducer.class);

  private final Configuration _conf;
  private final String _topic;
  private final KafkaProducer<Integer, KeyCoordinatorQueueMsg> _kafkaProducer;

  public KeyCoordinatorQueueProducer(Configuration conf, String hostname) {
    _conf = conf;
    _topic = _conf.getString(CommonConfig.KAFKA_CONFIG.TOPIC_KEY);
    final Properties kafkaProducerConfig = CommonUtils.getPropertiesFromConf(
        conf.subset(CommonConfig.KAFKA_CONFIG.KAFKA_CONFIG_KEY));

    kafkaProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
    kafkaProducerConfig.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, IntPartitioner.class.getName());
    DistributedCommonUtils.setKakfaLosslessProducerConfig(kafkaProducerConfig, hostname);

    _kafkaProducer = new KafkaProducer<>(kafkaProducerConfig);
  }

  @Override
  protected KafkaProducer<Integer, KeyCoordinatorQueueMsg> getKafkaNativeProducer() {
    return _kafkaProducer;
  }

  @Override
  protected String getDefaultTopic() {
    return _topic;
  }
}

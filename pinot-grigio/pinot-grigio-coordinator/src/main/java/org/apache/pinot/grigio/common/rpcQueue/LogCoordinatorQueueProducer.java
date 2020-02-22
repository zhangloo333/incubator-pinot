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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.pinot.grigio.common.config.CommonConfig;
import org.apache.pinot.grigio.common.messages.LogCoordinatorMessage;
import org.apache.pinot.grigio.common.metrics.GrigioMetrics;
import org.apache.pinot.grigio.common.utils.CommonUtils;
import org.apache.pinot.grigio.common.CoordinatorConfig;
import org.apache.pinot.grigio.common.DistributedCommonUtils;
import org.apache.pinot.grigio.common.FixedPartitionCountIntPartitioner;

import java.util.Properties;

public class LogCoordinatorQueueProducer extends KafkaQueueProducer<Integer, LogCoordinatorMessage> {

  private KafkaProducer<Integer, LogCoordinatorMessage> _kafkaProducer;
  private String _topic;
  private GrigioMetrics _grigioMetrics;

  @Override
  protected KafkaProducer<Integer, LogCoordinatorMessage> getKafkaNativeProducer() {
    Preconditions.checkState(_kafkaProducer != null, "Producer has not been initialized yet");
    return _kafkaProducer;
  }

  @Override
  protected String getDefaultTopic() {
    Preconditions.checkState(_topic != null, "Producer has not been initialized yet");
    return _topic;
  }

  @Override
  protected GrigioMetrics getMetrics() {
    return _grigioMetrics;
  }

  @Override
  public void init(Configuration conf, GrigioMetrics grigioMetrics) {
    final Properties kafkaProducerConfig = CommonUtils.getPropertiesFromConf(
        conf.subset(CoordinatorConfig.KAFKA_CONFIG.KAFKA_CONFIG_KEY));
    String hostName = conf.getString(CommonConfig.RPC_QUEUE_CONFIG.HOSTNAME_KEY);
    _topic = conf.getString(CommonConfig.RPC_QUEUE_CONFIG.TOPIC_KEY);
    _grigioMetrics = grigioMetrics;

    kafkaProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
    kafkaProducerConfig.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, FixedPartitionCountIntPartitioner.class.getName());
    DistributedCommonUtils.setKakfaLosslessProducerConfig(kafkaProducerConfig, hostName);

    this._kafkaProducer = new KafkaProducer<>(kafkaProducerConfig);
  }
}

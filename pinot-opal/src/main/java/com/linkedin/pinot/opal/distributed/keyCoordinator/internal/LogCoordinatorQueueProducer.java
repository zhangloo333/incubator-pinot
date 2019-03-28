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

import com.linkedin.pinot.opal.common.RpcQueue.KafkaQueueProducer;
import com.linkedin.pinot.opal.common.RpcQueue.ProduceTask;
import com.linkedin.pinot.opal.common.messages.LogCoordinatorMessage;
import com.linkedin.pinot.opal.common.utils.CommonUtils;
import com.linkedin.pinot.opal.distributed.keyCoordinator.common.DistributedCommonUtils;
import com.linkedin.pinot.opal.distributed.keyCoordinator.serverIngestion.KeyCoordinatorQueueProducer;
import com.linkedin.pinot.opal.distributed.keyCoordinator.starter.KeyCoordinatorConf;
import org.apache.commons.configuration.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class LogCoordinatorQueueProducer extends KafkaQueueProducer<String, LogCoordinatorMessage> {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorQueueProducer.class);

  private final Configuration _conf;
  private final KafkaProducer<String, LogCoordinatorMessage> _kafkaProducer;

  public LogCoordinatorQueueProducer(Configuration conf, String hostName) {
    this._conf = conf;
    final Properties kafkaProducerConfig = CommonUtils.getPropertiesFromConf(
        conf.subset(KeyCoordinatorConf.KEY_COORDINATOR_KAFKA_CONF));
    kafkaProducerConfig.put("key.serializer", StringSerializer.class.getName());
    kafkaProducerConfig.put("value.serializer", LogCoordinatorMessage.LogCoordinatorMessageSerializer.class.getName());
    DistributedCommonUtils.setKakfaLosslessProducerConfig(kafkaProducerConfig, hostName);

    this._kafkaProducer = new KafkaProducer<>(kafkaProducerConfig);
  }

  @Override
  public void produce(ProduceTask<String, LogCoordinatorMessage> produceTask) {
    _kafkaProducer.send(new ProducerRecord<>(produceTask.getTopic(), produceTask.getKey(), produceTask.getValue()),
        produceTask::markComplete);
  }

  @Override
  public void batchProduce(List<ProduceTask<String, LogCoordinatorMessage>> produceTasks) {
    for (ProduceTask<String, LogCoordinatorMessage> task: produceTasks) {
      produce(task);
    }
  }

  @Override
  public void close() {
    _kafkaProducer.close();
  }
}

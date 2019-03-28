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
package com.linkedin.pinot.opal.distributed.keyCoordinator.serverIngestion;

import com.linkedin.pinot.opal.common.RpcQueue.KafkaQueueProducer;
import com.linkedin.pinot.opal.common.RpcQueue.ProduceTask;
import com.linkedin.pinot.opal.common.messages.KeyCoordinatorQueueMsg;
import com.linkedin.pinot.opal.common.utils.CommonUtils;
import com.linkedin.pinot.opal.distributed.keyCoordinator.common.DistributedCommonUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class KeyCoordinatorQueueProducer extends KafkaQueueProducer<byte[], KeyCoordinatorQueueMsg> {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorQueueProducer.class);

  private final Configuration _conf;
  private final String _topic;
  private final KafkaProducer<byte[], KeyCoordinatorQueueMsg> _kafkaProducer;

  public KeyCoordinatorQueueProducer(Configuration conf, String hostname) {
    _conf = conf;

    _topic = _conf.getString(ServerKeyCoordinatorConfig.KAFKA_TOPIC_CONFIG);
    final Properties kafkaProducerConfig = CommonUtils.getPropertiesFromConf(
        conf.subset(ServerKeyCoordinatorConfig.KAFKA_PRODUCER_CONFIG));

    kafkaProducerConfig.put("key.serializer", ByteArraySerializer.class.getName());
    kafkaProducerConfig.put("value.serializer", KeyCoordinatorQueueMsg.KeyCoordinatorQueueMsgSerializer.class.getName());
    DistributedCommonUtils.setKakfaLosslessProducerConfig(kafkaProducerConfig, hostname);

    this._kafkaProducer = new KafkaProducer<>(kafkaProducerConfig);
  }

  @Override
  public void produce(ProduceTask<byte[], KeyCoordinatorQueueMsg> produceTask) {
    _kafkaProducer.send(new ProducerRecord<>(_topic, produceTask.getKey(), produceTask.getValue()),
        produceTask::markComplete);
  }

  @Override
  public void batchProduce(List<ProduceTask<byte[], KeyCoordinatorQueueMsg>> produceTasks) {
    for (ProduceTask<byte[], KeyCoordinatorQueueMsg> task: produceTasks) {
      produce(task);
    }
  }

  public void flush() {
    _kafkaProducer.flush();
  }

  @Override
  public void close() {

  }
}

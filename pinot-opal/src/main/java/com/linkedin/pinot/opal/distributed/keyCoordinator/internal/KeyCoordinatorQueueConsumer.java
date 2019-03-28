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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.linkedin.pinot.opal.common.RpcQueue.KafkaQueueConsumer;
import com.linkedin.pinot.opal.common.messages.KeyCoordinatorQueueMsg;
import com.linkedin.pinot.opal.common.utils.CommonUtils;
import com.linkedin.pinot.opal.distributed.keyCoordinator.common.DistributedCommonUtils;
import com.linkedin.pinot.opal.distributed.keyCoordinator.starter.KeyCoordinatorConf;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class KeyCoordinatorQueueConsumer extends KafkaQueueConsumer<String, KeyCoordinatorQueueMsg> {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorQueueConsumer.class);

  private Configuration _conf;
  private String _topic;
  private List<String> _partitions;

  /**
   * @param conf configuration of the kafka key coordinator queue consumer
   * @param hostname the local host name
   */
  public KeyCoordinatorQueueConsumer(Configuration conf, String hostname) {
    _conf = conf;
    _topic = conf.getString(KeyCoordinatorConf.KEY_COORDINATOR_TOPIC);
    _partitions = conf.getList(KeyCoordinatorConf.KEY_COORDINATOR_PARTITIONS);
    Preconditions.checkState(StringUtils.isNotEmpty(_topic), "kafka consumer topic should not be empty");
    Preconditions.checkState(_partitions != null && _partitions.size() > 0, "kafka partitions list should not be empty");

    Properties kafkaProperties = CommonUtils.getPropertiesFromConf(conf.subset(KeyCoordinatorConf.KEY_COORDINATOR_KAFKA_CONF));
    kafkaProperties.put("key.deserializer", ByteArrayDeserializer.class.getName());
    kafkaProperties.put("value.deserializer", KeyCoordinatorQueueMsg.KeyCoordinatorQueueMsgDeserializer.class.getName());
    kafkaProperties.put("group.id", KeyCoordinatorConf.KAFKA_CONSUMER_GROUP_ID_PREFIX + hostname);
    kafkaProperties.put("client.id", DistributedCommonUtils.getClientId(hostname));
    kafkaProperties.put("enable.auto.commit", false);
    try {
      List<Integer> integerPartitions = _partitions.stream().map(Integer::parseInt).collect(Collectors.toList());
      init(kafkaProperties);
      subscribe(ImmutableMap.of(_topic, integerPartitions));
    } catch (NumberFormatException ex) {
      LOGGER.error("partitions is not number", ex);
    }
  }

  @Override
  public Logger getLogger() {
    return LOGGER;
  }
}

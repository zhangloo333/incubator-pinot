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
package org.apache.pinot.opal.distributed.keyCoordinator.serverUpdater;

import org.apache.pinot.opal.common.Config.CommonConfig;
import org.apache.pinot.opal.common.RpcQueue.KafkaQueueConsumer;
import org.apache.pinot.opal.common.messages.LogCoordinatorMessage;
import org.apache.pinot.opal.common.utils.CommonUtils;
import org.apache.pinot.opal.distributed.keyCoordinator.common.DistributedCommonUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * consumer to ingest data from key coordinator output kafka topic, run in segment updater to fetch all update events
 */
public class SegmentUpdateQueueConsumer extends KafkaQueueConsumer<String, LogCoordinatorMessage> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentUpdateQueueConsumer.class);

  private final Configuration _conf;
  private final String _topic;

  public SegmentUpdateQueueConsumer(Configuration conf) {
    _conf = conf;
    String hostName = conf.getString(CommonConfig.KAFKA_CONFIG.HOSTNAME_KEY);
    final String groupid = conf.getString(SegmentUpdaterQueueConfig.CONSUMER_GROUP_ID_PREFIX, SegmentUpdaterQueueConfig.CONSUMER_GROUP_ID_PREFIX_DEFAULT) + hostName;

    LOGGER.info("creating segment updater kafka consumer with group id {}", groupid);
    Properties kafkaProperties = CommonUtils.getPropertiesFromConf(conf.subset(SegmentUpdaterQueueConfig.KAFKA_CONSUMER_CONFIG));
    kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
    kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LogCoordinatorMessage.LogCoordinatorMessageDeserializer.class.getName());
    kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupid);
    kafkaProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, DistributedCommonUtils.getClientId(hostName));
    kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    _topic = conf.getString(SegmentUpdaterQueueConfig.TOPIC_CONFIG_KEY);
    try {
      init(kafkaProperties);
      this.subscribe(_topic);
    } catch (NumberFormatException ex) {
      LOGGER.error("partitions is not number", ex);
    }
  }

  @Override
  public Logger getLogger() {
    return LOGGER;
  }

}

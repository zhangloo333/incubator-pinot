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
package com.linkedin.pinot.opal.distributed.keyCoordinator.serverUpdater;

import com.linkedin.pinot.opal.common.RpcQueue.KafkaQueueConsumer;
import com.linkedin.pinot.opal.common.messages.LogCoordinatorMessage;
import com.linkedin.pinot.opal.common.utils.CommonUtils;
import com.linkedin.pinot.opal.distributed.keyCoordinator.common.DistributedCommonUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.linkedin.pinot.opal.distributed.keyCoordinator.serverUpdater.SegmentUpdaterQueueConfig.CONSUMER_GROUP_ID_PREFIX;
import static com.linkedin.pinot.opal.distributed.keyCoordinator.serverUpdater.SegmentUpdaterQueueConfig.CONSUMER_GROUP_ID_PREFIX_DEFAULT;
import static com.linkedin.pinot.opal.distributed.keyCoordinator.serverUpdater.SegmentUpdaterQueueConfig.TOPIC_CONFIG_KEY;

/**
 * consumer to ingest data from key coordinator output kafka topic, run in segment updater to fetch all update events
 */
public class SegmentUpdateQueueConsumer extends KafkaQueueConsumer<String, LogCoordinatorMessage> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentUpdateQueueConsumer.class);

  private final Configuration _conf;
  private final String _topic;

  public SegmentUpdateQueueConsumer(Configuration conf, String hostName) {
    _conf = conf;
    final String groupid = conf.getString(CONSUMER_GROUP_ID_PREFIX, CONSUMER_GROUP_ID_PREFIX_DEFAULT) + hostName;

    LOGGER.info("creating segment updater kafka consumer with group id {}", groupid);
    Properties kafkaProperties = CommonUtils.getPropertiesFromConf(conf.subset(SegmentUpdaterQueueConfig.KAFKA_CONSUMER_CONFIG));
    kafkaProperties.put("key.deserializer", StringDeserializer.class.getName());
    kafkaProperties.put("value.deserializer", LogCoordinatorMessage.LogCoordinatorMessageDeserializer.class.getName());
    kafkaProperties.put("group.id", groupid);
    kafkaProperties.put("client.id", DistributedCommonUtils.getClientId(hostName));
    kafkaProperties.put("enable.auto.commit", false);
    _topic = conf.getString(TOPIC_CONFIG_KEY);
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

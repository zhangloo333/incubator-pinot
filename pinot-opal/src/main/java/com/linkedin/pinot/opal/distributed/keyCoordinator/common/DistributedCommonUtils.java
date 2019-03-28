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
package com.linkedin.pinot.opal.distributed.keyCoordinator.common;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.opal.distributed.keyCoordinator.starter.KeyCoordinatorConf;

import java.util.Properties;

import static com.linkedin.pinot.opal.distributed.keyCoordinator.starter.KeyCoordinatorConf.KAFKA_CLIENT_ID_PREFIX;
import static com.linkedin.pinot.opal.distributed.keyCoordinator.starter.KeyCoordinatorConf.KAFKA_TOPIC_PREFIX;

public class DistributedCommonUtils {


  public static String getKafkaTopicFromTableName(String tableName) {
    return KAFKA_TOPIC_PREFIX + tableName;
  }

  public static String getTableNameFromKafkaTopic(String kafkaTopic) {
    Preconditions.checkState(kafkaTopic.length() > KAFKA_TOPIC_PREFIX.length(), "kafka topic is not valid");
    return kafkaTopic.substring(KAFKA_TOPIC_PREFIX.length());
  }

  public static String getClientId(String hostName) {
    return KAFKA_CLIENT_ID_PREFIX + hostName;
  }

  public static void setKakfaLosslessProducerConfig(Properties kafkaProducerConfig, String hostname) {
    kafkaProducerConfig.put("acks", "all");
    if (!kafkaProducerConfig.containsKey("retries")) {
      kafkaProducerConfig.put("retries", KeyCoordinatorConf.KAKFA_PRODUCER_RETRIES);
    }
    if (!kafkaProducerConfig.containsKey("linger.ms")) {
      kafkaProducerConfig.put("linger.ms", KeyCoordinatorConf.KAKFA_PRODUCER_LINGER_MS);
    }
    if (!kafkaProducerConfig.containsKey("block.on.buffer.full")) {
      kafkaProducerConfig.put("block.on.buffer.full", true);
    }
    if (!kafkaProducerConfig.containsKey("client.id")) {
      kafkaProducerConfig.put("client.id", DistributedCommonUtils.getClientId(hostname));
    }
  }
}

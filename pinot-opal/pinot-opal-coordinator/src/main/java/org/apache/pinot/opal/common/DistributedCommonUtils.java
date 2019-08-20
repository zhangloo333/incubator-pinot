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
package org.apache.pinot.opal.common;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.pinot.opal.common.config.CommonConfig;
import org.apache.pinot.opal.keyCoordinator.starter.KeyCoordinatorConf;

import java.util.Properties;

public class DistributedCommonUtils {

  public static String getKafkaTopicFromTableName(String tableName) {
    return CommonConfig.RPC_QUEUE_CONFIG.DEFAULT_KC_OUTPUT_TOPIC_PREFIX + tableName;
  }

  public static String getClientId(String hostName) {
    return KeyCoordinatorConf.KAFKA_CLIENT_ID_PREFIX + hostName;
  }

  public static void setKakfaLosslessProducerConfig(Properties kafkaProducerConfig, String hostname) {
    kafkaProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    if (!kafkaProducerConfig.containsKey(ProducerConfig.RETRIES_CONFIG)) {
      kafkaProducerConfig.put(ProducerConfig.RETRIES_CONFIG, CoordinatorConfig.KAFKA_CONFIG.PRODUCER_RETRIES);
    }
    if (!kafkaProducerConfig.containsKey(ProducerConfig.COMPRESSION_TYPE_CONFIG)) {
      kafkaProducerConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, CoordinatorConfig.KAFKA_CONFIG.COMPRESS_TYPE);
    }
    if (!kafkaProducerConfig.containsKey(ProducerConfig.LINGER_MS_CONFIG)) {
      kafkaProducerConfig.put(ProducerConfig.LINGER_MS_CONFIG, CoordinatorConfig.KAFKA_CONFIG.PRODUCER_LINGER_MS);
    }
    if (!kafkaProducerConfig.containsKey(ProducerConfig.MAX_BLOCK_MS_CONFIG)) {
      kafkaProducerConfig.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, CoordinatorConfig.KAFKA_CONFIG.PRODUCER_MAX_BLOCK_TIME_MS);
    }
    if (!kafkaProducerConfig.containsKey(ProducerConfig.CLIENT_ID_CONFIG)) {
      kafkaProducerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, DistributedCommonUtils.getClientId(hostname));
    }
  }
}

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
package org.apache.pinot.opal.distributed.keyCoordinator.starter;

import org.apache.pinot.opal.common.Config.CommonConfig;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.File;

public class KeyCoordinatorConf extends PropertiesConfiguration {

  public static final String FETCH_MSG_DELAY_MS = "kc.queue.fetch.delay.ms";
  public static final int FETCH_MSG_DELAY_MS_DEFAULT = 100;

  public static final String FETCH_MSG_MAX_DELAY_MS = "kc.queue.fetch.delay.max.ms";
  public static final int FETCH_MSG_MAX_DELAY_MS_DEFAULT = 5000;

  public static final String FETCH_MSG_MAX_BATCH_SIZE = "kc.queue.fetch.size";
  public static final int FETCH_MSG_MAX_BATCH_SIZE_DEFAULT = 10000;

  public static final String CONSUMER_BLOCKING_QUEUE_SIZE = "consumer.queue.size";
  public static final int CONSUMER_BLOCKING_QUEUE_SIZE_DEFAULT = 10000;

  public static final String KEY_COORDINATOR_KV_STORE = "kvstore";

  public static final String KEY_COORDINATOR_PARTITIONS = "partitions";

  // server related config
  public static final String SERVER_CONFIG = "web.server";
  public static final String PORT = "jersey.port";
  public static final int PORT_DEFAULT = 8092;
  public static final String HOST_NAME = "hostname";

  // storage provider config
  public static final String STORAGE_PROVIDER_CONFIG = "updatelog.storage";

  // kafka prefix
  public static final String KAFKA_TOPIC_PREFIX = "pinot_upsert_";
  public static final String KAFKA_CLIENT_ID_PREFIX = "pinot_upsert_client_";
  public static final String KAFKA_CONSUMER_GROUP_ID_PREFIX = "pinot_upsert_kc_consumerGroup_";

  public KeyCoordinatorConf(File file) throws ConfigurationException {
    super(file);
  }

  public KeyCoordinatorConf() {
    super();
  }

  public int getConsumerBlockingQueueSize() {
    return getInt(CONSUMER_BLOCKING_QUEUE_SIZE, CONSUMER_BLOCKING_QUEUE_SIZE_DEFAULT);
  }

  public Configuration getProducerConf() {
    return this.subset(CommonConfig.KAFKA_CONFIG.PRODUCER_CONFIG_KEY);
  }

  public Configuration getConsumerConf() {
    return this.subset(CommonConfig.KAFKA_CONFIG.CONSUMER_CONFIG_KEY);
  }

  public Configuration getServerConf() {
    return this.subset(SERVER_CONFIG);
  }

  public Configuration getStorageProviderConf() {
    return this.subset(STORAGE_PROVIDER_CONFIG);
  }
}

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
package org.apache.pinot.opal.common.Config;

public class CommonConfig {

  public static class KAFKA_CONFIG {
    public static final String KAFKA_CONFIG_KEY = "kafka.conf";
    public static final String TOPIC_KEY = "topic";
    public static final String HOSTNAME_KEY = "hostname";

    public static final int PRODUCER_LINGER_MS = 50;
    public static final String COMPRESS_TYPE = "snappy";
    public static final int PRODUCER_RETRIES = 3;
    public static final int PRODUCER_MAX_BLOCK_TIME_MS = 60000;

    public static final String PRODUCER_CONFIG_KEY = "producer";
    public static final String CONSUMER_CONFIG_KEY = "consumer";
    public static final String CONSUMER_GROUP_PREFIX_KEY = "group.prefix";
  }

  public static class ENCODER_CONFIG {
    public static final String ENCODER_CONFIG_KEY = "encoder";
  }
}

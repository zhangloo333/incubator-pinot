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

public class SegmentUpdaterQueueConfig {

  public static final String TOPIC_CONFIG_KEY = "topic";
  public static final String CONSUMER_CONFIG = "consumer";
  public static final String KAFKA_CONSUMER_CONFIG = "kafkaConfig";
  public static final String CONSUMER_GROUP_ID_PREFIX = "consumer.groupid";
  public static final String CONSUMER_GROUP_ID_PREFIX_DEFAULT = "pinot_upsert_updater_";
}

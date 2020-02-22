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
package org.apache.pinot.grigio.common.metrics;

import org.apache.pinot.common.Utils;
import org.apache.pinot.common.metrics.AbstractMetrics;
import org.apache.pinot.grigio.common.metrics.GrigioMetrics.MetricsType;

public enum GrigioTimer implements AbstractMetrics.Timer {

  // common metrics for kafka component used in upsert
  PRODUCER_LAG(MetricsType.BOTH),
  FLUSH_LAG(MetricsType.BOTH),
  COMMIT_OFFSET_LAG(MetricsType.BOTH),
  FETCH_MESSAGE_LAG(MetricsType.BOTH),

  // metrics for segment updater
  FETCH_MSG_FROM_CONSUMER_TIME(MetricsType.SERVER_ONLY),
  UPDATE_DATAMANAGER_TIME(MetricsType.SERVER_ONLY),
  UPDATE_LOCAL_LOG_FILE_TIME(MetricsType.SERVER_ONLY),
  SEGMENT_UPDATER_LOOP_TIME(MetricsType.SERVER_ONLY),

  // metrics for key coordinator
  MESSAGE_PROCESS_THREAD_FETCH_DELAY(MetricsType.KC_ONLY),
  MESSAGE_PROCESS_THREAD_PROCESS_DELAY(MetricsType.KC_ONLY),

  PRODUCE_VERSION_MESSAGE(MetricsType.KC_ONLY),

  FETCH_MSG_FROM_KV_DELAY(MetricsType.KC_ONLY),
  PROCESS_MSG_UPDATE(MetricsType.KC_ONLY),
  SEND_MSG_TO_KAFKA(MetricsType.KC_ONLY),
  STORE_UPDATE_ON_KV(MetricsType.KC_ONLY),
  STORE_UPDATE_ON_DISK(MetricsType.KC_ONLY),
  ;

  private final String _timerName;
  private final MetricsType _type;

  GrigioTimer(MetricsType type) {
    _timerName = Utils.toCamelCase(name().toLowerCase());
    _type = type;
  }

  @Override
  public String getTimerName() {
    return _timerName;
  }

  @Override
  public boolean isGlobal() {
    return false;
  }

  public MetricsType getType() {
    return _type;
  }
}

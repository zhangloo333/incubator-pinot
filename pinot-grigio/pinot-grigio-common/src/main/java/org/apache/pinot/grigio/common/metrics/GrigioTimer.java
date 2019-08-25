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

public enum GrigioTimer implements AbstractMetrics.Timer {

  // comment metrics for kafka component
  PRODUCER_LAG(),
  FLUSH_LAG(),
  COMMIT_OFFSET_LAG(),
  FETCH_MESSAGE_LAG(),

  // metrics for segment updater
  FETCH_MSG_FROM_CONSUMER_TIME(),
  UPDATE_DATAMANAGER_TIME(),
  UPDATE_LOCAL_LOG_FILE_TIME(),
  SEGMENT_UPDATER_LOOP_TIME()
  ;

  private final String _timerName;

  GrigioTimer() {
    this._timerName = Utils.toCamelCase(name().toLowerCase());
  }

  @Override
  public String getTimerName() {
    return _timerName;
  }

  @Override
  public boolean isGlobal() {
    return false;
  }
}

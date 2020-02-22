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

public enum GrigioMeter implements AbstractMetrics.Meter {

  // metrics for kafka consumer library used in upsert components (server, key coordinator)
  MESSAGE_INGEST_COUNT_PER_BATCH("message", MetricsType.BOTH),

  // segment updater metrics
  MESSAGE_FETCH_PER_ROUND("messages", MetricsType.SERVER_ONLY),

  // key coordinator related metrics
  MESSAGE_PRODUCE_FAILED_COUNT("message", MetricsType.KC_ONLY),
  MESSAGE_PROCESS_THREAD_FETCH_COUNT("messages", MetricsType.KC_ONLY)
  ;

  private final String _meterName;
  private final String _unit;
  private final MetricsType _type;

  GrigioMeter(String unit, MetricsType type) {
    this._unit = unit;
    this._meterName = Utils.toCamelCase(name().toLowerCase());
    this._type = type;
  }

  @Override
  public String getMeterName() {
    return _meterName;
  }

  @Override
  public String getUnit() {
    return _unit;
  }

  @Override
  public boolean isGlobal() {
    return true;
  }

  public MetricsType getType() {
    return _type;
  }
}

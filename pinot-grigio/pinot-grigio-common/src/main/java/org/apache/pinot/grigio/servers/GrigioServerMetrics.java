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
package org.apache.pinot.grigio.servers;

import com.yammer.metrics.core.MetricsRegistry;
import org.apache.pinot.grigio.common.metrics.GrigioGauge;
import org.apache.pinot.grigio.common.metrics.GrigioMeter;
import org.apache.pinot.grigio.common.metrics.GrigioMetrics;

public class GrigioServerMetrics extends GrigioMetrics {

  private GrigioMeter[] meters = filterMeterByTypes(MetricsType.BOTH, MetricsType.SERVER_ONLY);
  private GrigioGauge[] gauges = filterGaugeByTypes(MetricsType.BOTH, MetricsType.SERVER_ONLY);

  public GrigioServerMetrics(String prefix, MetricsRegistry metricsRegistry) {
    super(prefix, metricsRegistry);
  }

  @Override
  protected GrigioMeter[] getMeters() {
    return meters;
  }

  @Override
  protected GrigioGauge[] getGauges() {
    return gauges;
  }

}

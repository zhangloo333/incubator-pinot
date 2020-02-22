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

import com.yammer.metrics.core.MetricsRegistry;
import org.apache.pinot.common.metrics.AbstractMetrics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class GrigioMetrics extends AbstractMetrics<AbstractMetrics.QueryPhase, GrigioMeter, GrigioGauge, GrigioTimer> {

  public GrigioMetrics(String prefix, MetricsRegistry metricsRegistry) {
    super(prefix, metricsRegistry, GrigioMetrics.class);
  }

  @Override
  protected QueryPhase[] getQueryPhases() {
    return new QueryPhase[0];
  }

  public void addTimedValueMs(GrigioTimer timer, long duration) {
    addTimedValue(timer, duration, TimeUnit.MILLISECONDS);
  }

  public void addTimedTableValueMs(String table, GrigioTimer timer, long duration) {
    addTimedTableValue(table, timer, duration, TimeUnit.MILLISECONDS);
  }

  protected static GrigioMeter[] filterMeterByTypes(MetricsType... types) {
    GrigioMeter[] meters = GrigioMeter.values();
    List<GrigioMeter> matchedMeters = new ArrayList<>();
    for (GrigioMeter meter : meters) {
      for (MetricsType type : types) {
        if (meter.getType() == type) {
          matchedMeters.add(meter);
          break;
        }
      }
    }
    return matchedMeters.toArray(new GrigioMeter[]{});
  }

  protected static GrigioGauge[] filterGaugeByTypes(MetricsType... types) {
    GrigioGauge[] gauges = GrigioGauge.values();
    List<GrigioGauge> matchedGauges = new ArrayList<>();
    for (GrigioGauge gauge: gauges) {
      for (MetricsType type : types) {
        if (gauge.getType() == type) {
          matchedGauges.add(gauge);
          break;
        }
      }
    }
    return matchedGauges.toArray(new GrigioGauge[]{});
  }

  public enum MetricsType {
    SERVER_ONLY,
    KC_ONLY,
    BOTH
  }
}

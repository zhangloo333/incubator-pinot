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
package org.apache.pinot.server.upsert;

import com.yammer.metrics.core.MetricsRegistry;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixManager;
import org.apache.pinot.core.segment.updater.DefaultWaterMarkManager;
import org.apache.pinot.core.segment.updater.SegmentDeletionHandler;
import org.apache.pinot.core.segment.updater.UpsertComponentContainer;
import org.apache.pinot.core.segment.updater.WaterMarkManager;

public class DefaultUpsertComponentContainer implements UpsertComponentContainer {

  private final SegmentDeletionHandler deletionHandler = new SegmentDeletionHandler();
  private final WaterMarkManager watermarkManager = new DefaultWaterMarkManager();

  @Override
  public void registerMetrics(String prefix, MetricsRegistry registry) {
  }

  @Override
  public void init(Configuration config, HelixManager helixManager, String clusterName, String instanceName) {
  }

  @Override
  public SegmentDeletionHandler getSegmentDeletionHandler() {
    return deletionHandler;
  }

  @Override
  public WaterMarkManager getWatermarkManager() {
    return watermarkManager;
  }

  @Override
  public boolean isUpsertEnabled() {
    return false;
  }

  @Override
  public void startBackgroundThread() {

  }

  @Override
  public void stopBackgroundThread() {

  }

  @Override
  public void shutdown() {

  }


}

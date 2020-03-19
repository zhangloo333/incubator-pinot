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
package org.apache.pinot.core.segment.updater;

import com.yammer.metrics.core.MetricsRegistry;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixManager;

/**
 * contains all components related to upsert in pinot server
 */
public interface UpsertComponentContainer {

  /**
   * register pinot upsert component metrics to the given registry
   * @param prefix the prefix of all metrics
   * @param registry the registry we are going to register the metrics to
   */
  void registerMetrics(String prefix, MetricsRegistry registry);

  /**
   * initialize the upsert comonent container with necessary config and information
   * @param config the configuration for this upsert component
   * @param helixManager helix manager for the current pinot server helix state
   * @param clusterName helix cluster name for the current pinot cluster
   * @param instanceName the name of current pinot instance in this cluster
   */
  void init(Configuration config, HelixManager helixManager, String clusterName, String instanceName);

  /**
   * start any necessary background processing for this upsert component
   */
  void startBackgroundThread();

  /**
   * stop any necessary background processing for this upsert component
   */
  void stopBackgroundThread();

  /**
   * shutdown and clean up any state for this upsert component
   */
  void shutdown();

  /**
   * return a segment deletion callback component that should be invoked when pinot server removed a segment
   * from its internal storage (to DROPPED state)
   */
  SegmentDeletionHandler getSegmentDeletionHandler();

  /**
   * return the current watermark manager for this server
   */
  WaterMarkManager getWatermarkManager();

  /**
   * check if upsert is enable for the current pinot server
   */
  boolean isUpsertEnabled();
}

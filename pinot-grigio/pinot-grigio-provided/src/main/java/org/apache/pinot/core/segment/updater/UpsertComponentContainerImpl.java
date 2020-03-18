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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.yammer.metrics.core.MetricsRegistry;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixManager;
import org.apache.pinot.grigio.common.metrics.GrigioMetrics;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogStorageProvider;
import org.apache.pinot.grigio.common.storageProvider.retentionManager.UpdateLogRetentionManager;
import org.apache.pinot.grigio.common.storageProvider.retentionManager.UpdateLogRetentionManagerImpl;
import org.apache.pinot.grigio.common.utils.IdealStateHelper;
import org.apache.pinot.grigio.servers.GrigioServerMetrics;
import org.apache.pinot.grigio.servers.KeyCoordinatorProvider;
import org.apache.pinot.grigio.servers.SegmentUpdaterProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.pinot.common.utils.CommonConstants.Grigio.PINOT_UPSERT_SERVER_COMPONENT_PREFIX;

public class UpsertComponentContainerImpl implements UpsertComponentContainer {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertComponentContainerImpl.class);

  // config keys
  public static final String ENABLED_CONFIG_KEY = "enabled";
  public static final String STORAGE_CONFIG_KEY = "storage";
  public static final String HOST_NAME_CONFIG_KEY = "hostname";
  public static final String KC_CONFIG_KEY = "kc";
  public static final String UPDATER_CONFIG_KEY = "updater";

  private volatile boolean _isUpsertEnabled = false;
  private AtomicBoolean _inited = new AtomicBoolean(false);

  // members of related upsert components
  private Configuration _conf;
  private String _hostName;
  private GrigioMetrics _grigioMetrics;
  private KeyCoordinatorProvider _keyCoordinatorProvider;
  private SegmentUpdaterProvider _segmentUpdaterProvider;
  private SegmentUpdater _segmentUpdater;
  private UpdateLogRetentionManager _retentionManager;
  private SegmentDeletionHandler _segmentDeletionHandler;
  private WaterMarkManager _waterMarkManager;

  @Override
  public void registerMetrics(String prefix, MetricsRegistry registry) {
    _grigioMetrics = new GrigioServerMetrics(prefix + PINOT_UPSERT_SERVER_COMPONENT_PREFIX, registry);
    _grigioMetrics.initializeGlobalMeters();
  }

  @Override
  public void init(Configuration config, HelixManager helixManager, String clusterName, String instanceName) {
    Preconditions.checkState(!_inited.getAndSet(true), "cannot initialize upsert component twice");
    _isUpsertEnabled = config.getBoolean(ENABLED_CONFIG_KEY, false);
    if (_isUpsertEnabled) {
      LOGGER.info("initializing upsert components");
      _conf = config;
      _hostName = _conf.getString(HOST_NAME_CONFIG_KEY);
      initVirtualColumnStorageProvider(config);
      _keyCoordinatorProvider = buildKeyCoordinatorProvider(config, _hostName);
      _segmentUpdaterProvider = buildSegmentUpdaterProvider(config, _hostName);
      _retentionManager = new UpdateLogRetentionManagerImpl(
          new IdealStateHelper(helixManager.getClusterManagmentTool(), clusterName), instanceName
      );
      _segmentUpdater = buildSegmentUpdater(config, _segmentUpdaterProvider, _retentionManager);
      UpsertWaterMarkManager.init(_grigioMetrics);
      _waterMarkManager = UpsertWaterMarkManager.getInstance();
      _segmentDeletionHandler = new SegmentDeletionHandler(ImmutableList.of(_segmentUpdater));
    } else {
      _waterMarkManager = new DefaultWaterMarkManager();
      _segmentDeletionHandler = new SegmentDeletionHandler();
    }
    _inited.set(true);
  }

  @Override
  public SegmentDeletionHandler getSegmentDeletionHandler() {
    Preconditions.checkState(_inited.get(), "upsert container is not initialized yet");
    return _segmentDeletionHandler;
  }

  @Override
  public WaterMarkManager getWatermarkManager() {
    return _waterMarkManager;
  }

  @Override
  public synchronized void startBackgroundThread() {
    if (_segmentUpdater != null) {
      _segmentUpdater.start();
    }
  }

  @Override
  public synchronized void stopBackgroundThread() {
    if (_segmentUpdater != null) {
      LOGGER.info("closing segment updater");
      _segmentUpdater.shutdown();
    }
  }

  @Override
  public boolean isUpsertEnabled() {
    return _isUpsertEnabled;
  }

  public synchronized void shutdown() {
    if (_keyCoordinatorProvider != null) {
      LOGGER.info("shutting down key coordinator provider");
      _keyCoordinatorProvider.close();
    }
  }

  private void initVirtualColumnStorageProvider(Configuration conf) {
    UpdateLogStorageProvider.init(conf.subset(STORAGE_CONFIG_KEY));
  }

  public KeyCoordinatorProvider buildKeyCoordinatorProvider(Configuration conf, String hostname) {
    return new KeyCoordinatorProvider(conf.subset(KC_CONFIG_KEY), hostname, _grigioMetrics);
  }

  public SegmentUpdaterProvider buildSegmentUpdaterProvider(Configuration conf, String hostname) {
    return new SegmentUpdaterProvider(conf.subset(UPDATER_CONFIG_KEY), hostname, _grigioMetrics);
  }

  public SegmentUpdater buildSegmentUpdater(Configuration conf, SegmentUpdaterProvider updateProvider,
                                            UpdateLogRetentionManager updateLogRetentionManager) {
    return new SegmentUpdater(conf.subset(UPDATER_CONFIG_KEY), updateProvider, updateLogRetentionManager,
            _grigioMetrics);
  }
}

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
package org.apache.pinot.grigio.common.storageProvider.retentionManager;

import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.Utils;
import org.apache.pinot.grigio.common.utils.IdealStateHelper;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogStorageProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KCUpdateLogRetentionManagerImpl implements UpdateLogRetentionManager, IdealStateChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateLogRetentionManagerImpl.class);

  private Map<String, UpdateLogTableRetentionManager> _retentionManagerMap = new ConcurrentHashMap<>();
  private Map<String, IdealState> _idealStateCache = new ConcurrentHashMap<>();
  private IdealStateHelper _idealStateHelper;
  private String _instanceName;
  private UpdateLogStorageProvider _provider;

  public KCUpdateLogRetentionManagerImpl(IdealStateHelper idealStateHelper, UpdateLogStorageProvider updateLogStorageProvider, String instanceName) {
    _idealStateHelper = idealStateHelper;
    _instanceName = instanceName;
    _provider = updateLogStorageProvider;
  }

  @Override
  public void onIdealStateChange(List<IdealState> idealState, NotificationContext changeContext)
      throws InterruptedException {
    Map<String, IdealState> tablesInChange = new HashMap<>();
    idealState.forEach(is -> tablesInChange.put(is.getResourceName(), is));
    for (Map.Entry<String, IdealState> entry: tablesInChange.entrySet()) {
      String tableNameWithType = entry.getKey();
      IdealState newIdealState = entry.getValue();
      IdealState oldIdealState = _idealStateCache.get(tableNameWithType);
      if (_retentionManagerMap.containsKey(tableNameWithType) && !newIdealState.equals(oldIdealState)) {
        LOGGER.info("updating table {} due to ideal state change notification", tableNameWithType);
        _retentionManagerMap.get(tableNameWithType).notifySegmentsChange();
      }
      _idealStateCache.put(tableNameWithType, newIdealState);
    }
  }

  @Override
  public UpdateLogTableRetentionManager getRetentionManagerForTable(String tableNameWithType) {
    return _retentionManagerMap.computeIfAbsent(tableNameWithType,
        t -> {
          try {
            return new KCUpdateLogTableRetentionManagerImpl(_idealStateHelper, t, _provider);
          } catch (IOException e) {
            LOGGER.error("failed to get retention manager for table {}", tableNameWithType, e);
            Utils.rethrowException(e);
          }
          // won't reach here
          return null;
        });
  }
}

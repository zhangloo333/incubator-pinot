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

import org.apache.pinot.grigio.common.utils.IdealStateHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class UpdateLogRetentionManagerImpl implements UpdateLogRetentionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateLogRetentionManagerImpl.class);

  private Map<String, UpdateLogTableRetentionManager> _retentionManagerMap = new ConcurrentHashMap<>();
  private IdealStateHelper _idealStateHelper;
  private String _instanceName;

  public UpdateLogRetentionManagerImpl(IdealStateHelper idealStateHelper, String instanceName) {
    _idealStateHelper = idealStateHelper;
    _instanceName = instanceName;
  }

  @Override
  public UpdateLogTableRetentionManager getRetentionManagerForTable(String tableNameWithType) {
    return _retentionManagerMap.computeIfAbsent(tableNameWithType,
        t -> new UpdateLogTableRetentionManagerImpl(_idealStateHelper, t, _instanceName));
  }
}

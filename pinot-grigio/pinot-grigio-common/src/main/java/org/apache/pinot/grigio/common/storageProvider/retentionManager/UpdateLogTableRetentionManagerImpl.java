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

import java.util.Arrays;

public class UpdateLogTableRetentionManagerImpl extends UpdateLogTableRetentionManager {

  protected String _instanceId;

  public UpdateLogTableRetentionManagerImpl(IdealStateHelper idealStateHelper, String tableName, String instanceId) {
    super(idealStateHelper, tableName);
    _instanceId = instanceId;
    updateStateFromHelix();
  }

  @Override
  public synchronized void notifySegmentDeletion(String segmentName) {
    LOGGER.info("handling segment deletion in retention manager");
    updateStateFromHelix();
    if (isSegmentAssignedToCurrentServer(segmentName)) {
      LOGGER.error("segment {} data manager has been removed but still in ideal state", segmentName);
      LOGGER.error("segment ideal state {}", Arrays.toString(_segmentsToInstanceMap.get(segmentName).entrySet().toArray()));
    }
  }

  @Override
  public void notifySegmentsChange() {
    updateStateFromHelix();
  }

  @Override
  protected boolean isSegmentAssignedToCurrentServer(String segmentName) {
    return _segmentsToInstanceMap.containsKey(segmentName)
        && _segmentsToInstanceMap.get(segmentName).containsKey(_instanceId)
        && !"DROPPED".equals(_segmentsToInstanceMap.get(segmentName).get(_instanceId));
  }

}

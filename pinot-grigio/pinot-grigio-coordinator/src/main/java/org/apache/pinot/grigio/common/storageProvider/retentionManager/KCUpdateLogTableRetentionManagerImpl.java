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

import com.google.common.collect.Sets;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogStorageProvider;
import org.apache.pinot.grigio.common.utils.IdealStateHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

public class KCUpdateLogTableRetentionManagerImpl extends UpdateLogTableRetentionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(KCUpdateLogTableRetentionManagerImpl.class);

  private Set<String> _segments;
  private final UpdateLogStorageProvider _provider;

  public KCUpdateLogTableRetentionManagerImpl(IdealStateHelper idealStateHelper, String tableName, UpdateLogStorageProvider provider) throws IOException {
    super(idealStateHelper, tableName);
    _provider = provider;
    // load the current update log on this server and match it with helix stored state,
    // so we can remove any unused update logs
    _provider.loadTable(tableName);
    _segments = _provider.getAllSegments(tableName);
    updateStateFromHelix();
  }

  public synchronized void updateSegmentsAndRemoveOldFiles(Set<String> newSegmentSet) {
    Set<String> segmentDeleted = Sets.difference(_segments, newSegmentSet);
    _segments = newSegmentSet;
    if (segmentDeleted.size() > 0) {
      LOGGER.info("deleting table {} segments {} from KC", _tableName, Arrays.toString(segmentDeleted.toArray()));
      segmentDeleted.forEach(segmentName -> {
        try {
          _provider.removeSegment(_tableName, segmentName);
        } catch (IOException e) {
          LOGGER.error("failed to remove segment for table {} segment {}", _tableName, segmentName);
        }
      });
    }
  }

  @Override
  protected boolean isSegmentAssignedToCurrentServer(String segmentName) {
    // always return true as key coordinator should store whatever segment sent to it
    return true;
  }

  @Override
  protected void updateStateFromHelix() {
    long start = System.currentTimeMillis();
    super.updateStateFromHelix();
    updateSegmentsAndRemoveOldFiles(_segmentsToInstanceMap.keySet());
    LOGGER.info("updated table {} state from helix in {} ms", _tableName, System.currentTimeMillis() - start);
  }

  @Override
  public void notifySegmentDeletion(String segmentName) {
    // do nothing, kc don't auto delete physical data
  }

  @Override
  public void notifySegmentsChange() {
    updateStateFromHelix();
  }
}

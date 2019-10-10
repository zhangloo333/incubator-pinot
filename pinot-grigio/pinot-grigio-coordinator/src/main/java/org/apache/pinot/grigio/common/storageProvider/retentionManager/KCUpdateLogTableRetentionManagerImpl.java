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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.pinot.common.utils.IdealStateHelper;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogStorageProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class KCUpdateLogTableRetentionManagerImpl implements UpdateLogTableRetentionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(KCUpdateLogTableRetentionManagerImpl.class);

  private Set<String> _segments;
  private Map<Integer, LLCSegmentName> _partitionToLastSegment;
  private final Map<String, String> _blackListedSegments;
  private final String _tableName;
  private final UpdateLogStorageProvider _provider;
  private final IdealStateHelper _idealStateHelper;

  public KCUpdateLogTableRetentionManagerImpl(IdealStateHelper idealStateHelper, String tableName, UpdateLogStorageProvider provider) throws IOException {

    _provider = provider;
    _tableName = tableName;
    _idealStateHelper = idealStateHelper;
    _blackListedSegments = new ConcurrentHashMap<>();
    _provider.loadTable(tableName);
    _segments = _provider.getAllSegments(tableName);
    _partitionToLastSegment = UpdateLogTableRetentionManager.getPartitionToLastSegment(_segments);
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

  private synchronized void updateSegments(Set<String> newSegmentSet) {
    _segments = newSegmentSet;
    _partitionToLastSegment = UpdateLogTableRetentionManager.getPartitionToLastSegment(_segments);
  }

  public Set<String> getAllSegments() {
    return ImmutableSet.copyOf(_segments);
  }

  @Override
  public synchronized boolean shouldIngestForSegment(String segmentName) {
    if (_segments.contains(segmentName)) {
      return true;
    } else if (_blackListedSegments.containsKey(segmentName)) {
      return false;
    } else {
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      int partition = llcSegmentName.getPartitionId();
      if (!_partitionToLastSegment.containsKey(llcSegmentName.getPartitionId())
          || UpdateLogTableRetentionManager.compareSegment(llcSegmentName, _partitionToLastSegment.get(partition))) {
        updateStateFromHelix();
        if (_segments.contains(segmentName)) {
          LOGGER.info("segment {} matched in ideal state after refresh", segmentName);
          return true;
        }
      }
      _blackListedSegments.put(segmentName, segmentName);
      return false;
    }
  }

  private void updateStateFromHelix() {
    long start = System.currentTimeMillis();
    updateSegmentsAndRemoveOldFiles(_idealStateHelper.getSegmentToInstanceMap(_tableName).keySet());
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

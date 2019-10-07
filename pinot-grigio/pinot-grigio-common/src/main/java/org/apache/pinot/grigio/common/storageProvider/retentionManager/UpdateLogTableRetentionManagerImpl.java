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

import com.google.common.collect.ImmutableMap;
import org.apache.pinot.common.utils.IdealStateHelper;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class UpdateLogTableRetentionManagerImpl implements UpdateLogTableRetentionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateLogTableRetentionManagerImpl.class);

  protected IdealStateHelper _idealStateHelper;
  protected String _tableName;

  private Map<Integer, LLCSegmentName> _partitionLastSeg = new ConcurrentHashMap<>();
  private Map<String, Map<String, String>> _segmentsToInstanceMap;
  // use the concurrent hashmap as concurrent hashset
  private Map<String, String> _blacklistedSegmentNames = new ConcurrentHashMap<>();
  private String _instanceId;

  public UpdateLogTableRetentionManagerImpl(IdealStateHelper idealStateHelper, String tableName, String instanceId) {
    _idealStateHelper = idealStateHelper;
    _tableName = tableName;
    _instanceId = instanceId;
    updateStateFromHelix();
  }

  private void updateStateFromHelix() {
    long start = System.currentTimeMillis();
    _segmentsToInstanceMap = ImmutableMap.copyOf(_idealStateHelper.getSegmentToInstanceMap(_tableName));
    if (_segmentsToInstanceMap.size() == 0) {
      LOGGER.error("failed to get any segment for the current table {}", _tableName);
    }
    Map<Integer, LLCSegmentName> partitionLastSeg = new HashMap<>();
    for (String segmentStr: _segmentsToInstanceMap.keySet()) {
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentStr);
      int partition = llcSegmentName.getPartitionId();
      if (!partitionLastSeg.containsKey(partition)) {
        partitionLastSeg.put(partition, llcSegmentName);
      } else if (partitionLastSeg.get(partition).getSequenceNumber() < llcSegmentName.getSequenceNumber()) {
        partitionLastSeg.put(partition, llcSegmentName);
      }
    }
    _partitionLastSeg = ImmutableMap.copyOf(partitionLastSeg);
    LOGGER.info("updated table {} state from helix in {} ms", _tableName, System.currentTimeMillis() - start);
  }

  @Override
  public synchronized boolean shouldIngestForSegment(String segmentName) {
    if (_segmentsToInstanceMap.containsKey(segmentName)) {
      return isSegmentAssignedToCurrentServer(segmentName);
    } else if (_blacklistedSegmentNames.containsKey(segmentName)) {
      return false;
    } else {
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      int partitionId = llcSegmentName.getPartitionId();
      int seq = llcSegmentName.getSequenceNumber();
      long ts = llcSegmentName.getCreationTimeStamp();
      if (_partitionLastSeg.containsKey(partitionId)) {
        LLCSegmentName latestSegment = _partitionLastSeg.get(partitionId);
        if (seq > latestSegment.getSequenceNumber() && ts > latestSegment.getCreationTimeStamp()) {
          // assume our idealState is out of date
          updateStateFromHelix();
          if (isSegmentAssignedToCurrentServer(segmentName)) {
            LOGGER.info("segment {} matched in ideal state after refresh", segmentName);
            return true;
          }
        } else {
          // we most probably got a segment that is from a deleted table or segment assigned to another table
          // assume we don't do re-balance, we won't do refresh
          LOGGER.info("adding segment {} to blacklist segment list", segmentName);
          _blacklistedSegmentNames.put(segmentName, segmentName);
        }
      }
    }
    return false;
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

  private boolean isSegmentAssignedToCurrentServer(String segmentName) {
    return _segmentsToInstanceMap.containsKey(segmentName)
        && _segmentsToInstanceMap.get(segmentName).containsKey(_instanceId)
        && !"DROPPED".equals(_segmentsToInstanceMap.get(segmentName).get(_instanceId));
  }

}

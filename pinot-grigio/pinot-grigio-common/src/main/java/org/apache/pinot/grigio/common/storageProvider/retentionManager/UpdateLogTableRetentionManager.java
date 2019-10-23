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
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.grigio.common.utils.IdealStateHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * class to help decide whether to keep update log for a particular segment or not
 */
public abstract class UpdateLogTableRetentionManager {

  protected static final Logger LOGGER = LoggerFactory.getLogger(UpdateLogTableRetentionManagerImpl.class);

  protected String _tableName;
  protected IdealStateHelper _idealStateHelper;
  // use the concurrent hashmap as concurrent hashset
  protected Map<Integer, LLCSegmentName> _partitionToLastSegment = new ConcurrentHashMap<>();
  protected Map<String, String> _blacklistedSegments = new ConcurrentHashMap<>();
  protected Map<String, Map<String, String>> _segmentsToInstanceMap;

  public UpdateLogTableRetentionManager(IdealStateHelper idealStateHelper, String tableName) {
    _idealStateHelper = idealStateHelper;
    _tableName = tableName;
  }

  /**
   * for external components to notify retention manager that physical data for this segment has been deleted
   * @param segmentName the name of the segment that we are deleting from local storage
   */
  public abstract void notifySegmentDeletion(String segmentName);

  /**
   * for external components to notify us that segments for this tables has been change and we should refresh it
   */
  public abstract void notifySegmentsChange();

  /**
   * check if the the given segment is assigned to current server
   */
  protected abstract boolean isSegmentAssignedToCurrentServer(String segmentName);

  /**
   * update state from helix
   */
  protected void updateStateFromHelix() {
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
    _partitionToLastSegment = ImmutableMap.copyOf(partitionLastSeg);
    LOGGER.info("updated table {} state from helix in {} ms", _tableName, System.currentTimeMillis() - start);
  }

  /**
   * decide whether we should ingest the update log for a given segment name
   * @param segmentName
   * @return true if we should keep the update log for a particular segment, false otherwise
   */
  public boolean shouldIngestForSegment(String segmentName) {
    if (_segmentsToInstanceMap.containsKey(segmentName)) {
      return isSegmentAssignedToCurrentServer(segmentName);
    } else if (_blacklistedSegments.containsKey(segmentName)) {
      return false;
    } else {
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      int partition = llcSegmentName.getPartitionId();
      if (!_partitionToLastSegment.containsKey(llcSegmentName.getPartitionId())
          || compareSegment(llcSegmentName, _partitionToLastSegment.get(partition))) {
        // assume our idealState is out of date
        updateStateFromHelix();
        if (_segmentsToInstanceMap.containsKey(segmentName) && isSegmentAssignedToCurrentServer(segmentName)) {
          LOGGER.info("segment {} matched in ideal state after refresh", segmentName);
          return true;
        }
      }
      // we most probably got a segment that is from a deleted table or segment assigned to another table
      // assume we don't do re-balance, we won't do refresh
      _blacklistedSegments.put(segmentName, segmentName);
      return false;
    }
  }

  /**
   * compare if segment1 is definitely newer segment compared to the segment2
   * @param segment1
   * @param segment2
   * @return true if the segment1 is a "newer" segment
   */
  protected boolean compareSegment(LLCSegmentName segment1, LLCSegmentName segment2) {
    return segment1.getCreationTimeStamp() > segment2.getCreationTimeStamp();
  }

}

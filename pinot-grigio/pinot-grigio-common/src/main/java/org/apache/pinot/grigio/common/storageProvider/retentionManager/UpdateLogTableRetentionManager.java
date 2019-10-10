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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * class to help decide whether to keep update log for a particular segment or not
 */
public interface UpdateLogTableRetentionManager {

  /**
   * decide whether we should ingest the update log for a given segment name
   * @param segmentName
   * @return true if we should keep the update log for a particular segment, false otherwise
   */
  boolean shouldIngestForSegment(String segmentName);

  /**
   * for external components to notify retention manager that physical data for this segment has been deleted
   * @param segmentName the name of the segment that we are deleting from local storage
   */
  void notifySegmentDeletion(String segmentName);

  /**
   * for external components to notify us that segments for this tables has been change and we should refresh it
   */
  void notifySegmentsChange();

  /**
   * get the mapping between the partition to the last segment (based on timestamp)
   * @param segments the list of segment we are comparing
   * @return the mapping between the partition id and the last segment of that partition
   */
  static Map<Integer, LLCSegmentName> getPartitionToLastSegment(Set<String> segments) {
    Map<Integer, LLCSegmentName> partitionToLastSegment = new HashMap<>();
    for (String segment : segments) {
      LLCSegmentName llcSegmentName = new LLCSegmentName(segment);
      int partition = llcSegmentName.getPartitionId();
      if (!partitionToLastSegment.containsKey(partition)) {
        partitionToLastSegment.put(partition, llcSegmentName);
      } else {
        LLCSegmentName currLastSegment = partitionToLastSegment.get(partition);
        if (llcSegmentName.getCreationTimeStamp() > currLastSegment.getCreationTimeStamp()) {
          partitionToLastSegment.put(partition, llcSegmentName);
        }
      }
    }
    return ImmutableMap.copyOf(partitionToLastSegment);
  }

  /**
   * compare if segment1 is definitely newer segment compared to the segment2
   * @param segment1
   * @param segment2
   * @return true if the segment1 is a "newer" segment
   */
  static boolean compareSegment(LLCSegmentName segment1, LLCSegmentName segment2) {
    return segment1.getSequenceNumber() > segment2.getSequenceNumber() &&
        segment1.getCreationTimeStamp() > segment2.getCreationTimeStamp();
  }

}

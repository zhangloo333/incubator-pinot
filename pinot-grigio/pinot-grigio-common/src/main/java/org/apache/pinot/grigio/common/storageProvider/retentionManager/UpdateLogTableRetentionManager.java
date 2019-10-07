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
}

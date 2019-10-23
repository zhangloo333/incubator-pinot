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
package org.apache.pinot.grigio.common.utils;

import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * class for getting ideal state from helix, used in update log retention management component to get up-to-date
 * view of the cluster assignment as helix notification/data manager assignment could be delayed for various reasons
 */
public class IdealStateHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(IdealStateHelper.class);

  private final String _clusterName;
  private final HelixAdmin _helixAdmin;

  public IdealStateHelper(HelixAdmin helixAdmin, String clusterName) {
    _clusterName = clusterName;
    _helixAdmin = helixAdmin;
  }

  private IdealState getResourceIdealState(String resourceName) {
    return _helixAdmin.getResourceIdealState(_clusterName, resourceName);
  }

  public Set<String> getAllSegmentsForTable(String tableNameWithType) {
    IdealState idealState = getResourceIdealState(tableNameWithType);
    if (idealState == null || !idealState.isEnabled()) {
      LOGGER.warn("ideal state for table {} is not found", tableNameWithType);
      return new HashSet<>();
    } else {
      return idealState.getPartitionSet();
    }
  }

  /**
   * fetch the mapping of {segmentName: {instanceId: helixState}} to the caller for a given table
   * the information will be the ideal state info stored on helix zk node
   * @param tableNameWithType the name of the table with type
   * @return
   */
  public Map<String, Map<String, String>> getSegmentToInstanceMap(String tableNameWithType) {
    Map<String, Map<String, String>> segmentToInstanceMap = new HashMap<>();
    IdealState idealState = getResourceIdealState(tableNameWithType);
    if (idealState == null || !idealState.isEnabled()) {
      LOGGER.warn("ideal state for table {} is not found or disabled", tableNameWithType);
    } else {
      for (String partitionName: idealState.getPartitionSet()) {
        segmentToInstanceMap.put(partitionName, idealState.getInstanceStateMap(partitionName));
      }
    }
    return segmentToInstanceMap;
  }
}

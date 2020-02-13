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
package org.apache.pinot.core.segment.updater;

import com.google.common.collect.ImmutableSet;
import org.apache.pinot.core.data.manager.UpsertSegmentDataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * class to manage list of data managers and their associated table/segment for segment updater
 */
@ThreadSafe
public class SegmentUpdaterDataManagerHolder {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentUpdaterDataManagerHolder.class);

  private final Map<String, Map<String, Set<UpsertSegmentDataManager>>> _tableSegmentMap = new ConcurrentHashMap<>();

  public SegmentUpdaterDataManagerHolder() {}

  /**
   * fetch all tables containing at least one data manager on this server
   * @return list of all pinot tables name for the current segment updater
   */
  public Set<String> getAllTables() {
    return ImmutableSet.copyOf(_tableSegmentMap.keySet());
  }

  /**
   * check if there is any data manager associated with the given table
   */
  public boolean hasTable(String tableName) {
    return _tableSegmentMap.containsKey(tableName);
  }

  /**
   * get a set of data manager for the given table name and segment name
   */
  public synchronized Set<UpsertSegmentDataManager> getDataManagers(String tableName, String segmentName) {
    if (!_tableSegmentMap.containsKey(tableName)) {
      LOGGER.error("try to fetch data manager for non-existing table {} segment {}", tableName, segmentName);
    } else {
      final Map<String, Set<UpsertSegmentDataManager>> segmentDataManagerMap = _tableSegmentMap.get(tableName);
      if (segmentDataManagerMap.containsKey(segmentName)) {
        return ImmutableSet.copyOf(segmentDataManagerMap.get(segmentName));
      }
    }
    return ImmutableSet.of();
  }

  /**
   * add a data manager for a given table and segment name
   */
  public synchronized void addDataManager(String tableName, String segmentName, UpsertSegmentDataManager dataManager) {
    LOGGER.info("adding new data manager to updater for table {}, segment {}", tableName, segmentName);
    if (!_tableSegmentMap.containsKey(tableName)) {
      _tableSegmentMap.put(tableName, new ConcurrentHashMap<>());
    }
    _tableSegmentMap.get(tableName).computeIfAbsent(segmentName, sn -> ConcurrentHashMap.newKeySet()).add(dataManager);
  }

  /**
   * remove a specific data manager for a given table and segment name.
   * do nothing if there is no such data manager for the given table/segment name
   */
  public synchronized void removeDataManager(String tableName, String segmentName,
                                             UpsertSegmentDataManager toDeleteManager) {
    Map<String, Set<UpsertSegmentDataManager>> segmentMap = _tableSegmentMap.get(tableName);
    if (segmentMap != null) {
      Set<UpsertSegmentDataManager> segmentDataManagers = segmentMap.get(segmentName);
      if (segmentDataManagers != null) {
        segmentDataManagers.remove(toDeleteManager);
        LOGGER.info("removing data manager for table {} segment {}", tableName, segmentName);
        if (segmentDataManagers.size() == 0) {
          segmentMap.remove(segmentName);
        }
      }
    }
  }

  /**
   * remove all data managers for a table and segment
   * @return true if we indeed remove any data manager, false otherwise
   */
  public synchronized boolean removeAllDataManagerForSegment(String tableName, String segmentName) {
    Map<String, Set<UpsertSegmentDataManager>> segmentManagerMap = _tableSegmentMap.get(tableName);
    if (segmentManagerMap != null) {
      if (segmentManagerMap.containsKey(segmentName)) {
        LOGGER.error("trying to remove segment storage with {} segment data manager", segmentManagerMap.get(segmentName).size());
      }
      Set<UpsertSegmentDataManager> result = segmentManagerMap.remove(segmentName);
      return result != null;
    }
    return false;
  }

  /**
   * check if the table still has any associated data manager. If there is no data managers, then removed it from cached
   * @return true if the given table is removed, false otherwise
   */
  public synchronized boolean maybeRemoveTable(String tableName) {
    Map<String, Set<UpsertSegmentDataManager>> segmentManagerMap = _tableSegmentMap.get(tableName);
    if (segmentManagerMap != null && segmentManagerMap.size() == 0) {
      _tableSegmentMap.remove(tableName);
      return true;
    }
    return false;
  }

}

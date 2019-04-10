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
package com.linkedin.pinot.core.segment.updater;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.linkedin.pinot.opal.common.StorageProvider.UpdateLogEntry;
import com.linkedin.pinot.opal.common.utils.PartitionIdMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class UpsertWaterMarkManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertWaterMarkManager.class);

  private final PartitionIdMapper partitionIdMapper = new PartitionIdMapper();
  private final Map<String, Map<Integer, Long>> _highWaterMarkTablePartitionMap = new ConcurrentHashMap<>();

  private static volatile UpsertWaterMarkManager _instance;

  private UpsertWaterMarkManager() {
  }

  public static UpsertWaterMarkManager getInstance() {
    if (_instance == null) {
      synchronized (UpsertWaterMarkManager.class) {
        if (_instance == null) {
          _instance = new UpsertWaterMarkManager();
        }
      }
    }
    return _instance;
  }

  public void processMessage(String table, String segment, UpdateLogEntry logEntry) {
    if (logEntry == null) {
      return;
    }
    long newOffset = logEntry.getValue();
    int partition = partitionIdMapper.getPartitionFromLLRealtimeSegment(segment);

    Map<Integer, Long> partitionToHighWaterMark = _highWaterMarkTablePartitionMap.computeIfAbsent(table, t -> new ConcurrentHashMap<>());
    partitionToHighWaterMark.compute(partition, (key, currentOffset) ->
        (currentOffset == null) ? newOffset: Math.max(newOffset, currentOffset));
  }

  public Map<Integer, Long> getHighWaterMarkForTable(String tableName) {
    return ImmutableMap.copyOf(_highWaterMarkTablePartitionMap.getOrDefault(tableName, ImmutableMap.of()));
  }

  public Map<String, Map<Integer, Long>> getHighWaterMarkTablePartitionMap() {
    return ImmutableMap.copyOf(_highWaterMarkTablePartitionMap);
  }

  @Override
  public String toString() {
    return Joiner.on(",").withKeyValueSeparator("=").join(_highWaterMarkTablePartitionMap);
  }
}

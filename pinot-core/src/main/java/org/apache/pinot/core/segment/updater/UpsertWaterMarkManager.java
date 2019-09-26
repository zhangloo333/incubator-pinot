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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.pinot.grigio.common.metrics.GrigioMeter;
import org.apache.pinot.grigio.common.metrics.GrigioMetrics;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class UpsertWaterMarkManager {

  private final Map<String, Map<Integer, Long>> _highWaterMarkTablePartitionMap = new ConcurrentHashMap<>();
  private final GrigioMetrics _metrics;

  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertWaterMarkManager.class);
  private static volatile UpsertWaterMarkManager _instance;

  private UpsertWaterMarkManager(GrigioMetrics metrics) {
    _metrics = metrics;
  }

  public static void init(GrigioMetrics metrics) {
    synchronized (UpsertWaterMarkManager.class) {
      Preconditions.checkState(_instance == null, "upsert water mark manager is already init");
      _instance = new UpsertWaterMarkManager(metrics);
    }
  }

  public static UpsertWaterMarkManager getInstance() {
    Preconditions.checkState(_instance != null, "upsert water mark manager is not yet init");
    return _instance;
  }

  // TODO(tingchen) Look into the case where Segment Update Messages might arrive before the corresponding physical data.
  public void processMessage(String table, String segment, UpdateLogEntry logEntry) {
    if (logEntry == null) {
      return;
    }
    long version = logEntry.getValue();
    int partition = logEntry.getPartition();
    Preconditions.checkState(partition >= 0, "logEntry has no partition info {} for table ", logEntry.toString(), table);

    Map<Integer, Long> partitionToHighWaterMark = _highWaterMarkTablePartitionMap.computeIfAbsent(table, t -> new ConcurrentHashMap<>());
    if (partitionToHighWaterMark.get(partition) == null) {
      partitionToHighWaterMark.put(partition, version);
    } else {
      long currentVersion = partitionToHighWaterMark.get(partition);
      if (version < currentVersion) {
        // We expect the version number to increase monotonically unless we are reprocessing previous seen messages.
        _metrics.addMeteredGlobalValue(GrigioMeter.VERSION_LOWER_THAN_CURRENT, 1);
        LOGGER.debug("The latest record {} has lower version than the current one for the table {} ", logEntry, table);
      }
      partitionToHighWaterMark.put(partition, Math.max(version, currentVersion));
    }
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

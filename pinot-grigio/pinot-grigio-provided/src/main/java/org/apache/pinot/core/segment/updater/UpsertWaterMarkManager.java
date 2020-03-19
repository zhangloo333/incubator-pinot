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
import org.apache.commons.configuration.Configuration;
import org.apache.pinot.grigio.common.metrics.GrigioGauge;
import org.apache.pinot.grigio.common.metrics.GrigioMetrics;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * watermark manager for upsert component to collect the low-water-mark information of each tables in the current
 * pinot server
 * watermark is defined as largest version of each partition (segment update event topic partition) for each table
 * so it stores the data in map of {table_name: {partition_id: highest_water_mark}}
 * then {@link LowWaterMarkService} will ingest those information from pinot server and calculate the lowest of these
 * watermark and use it in query to send to server
 */
public class UpsertWaterMarkManager implements WaterMarkManager {

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

  /**
   * process a event message and update the current watermark information for this manager
   * @param table
   * @param segment
   * @param logEntry the message containing the new watermark information
   */
  public void processMessage(String table, String segment, UpdateLogEntry logEntry) {
    if (logEntry == null) {
      return;
    }
    long version = logEntry.getValue();
    int partition = logEntry.getPartition();
    processVersionUpdate(table, partition, version);
  }

  /**
   * update the high watermark information associated with the given table/partition
   * @param table
   * @param partition
   * @param version
   */
  public void processVersionUpdate(String table, int partition, long version) {
    Preconditions.checkState(partition >= 0, "logEntry has invalid version {} for table {}",
        version, table);
    Map<Integer, Long> partitionToHighWaterMark = _highWaterMarkTablePartitionMap.computeIfAbsent(table, t -> new ConcurrentHashMap<>());
    long currentVersion = partitionToHighWaterMark.getOrDefault(partition, -1L);  // assumes that valid version is non-negative
    if (version > currentVersion) {
      partitionToHighWaterMark.put(partition, version);
      _metrics.setValueOfTableGauge(String.valueOf(partition), GrigioGauge.SERVER_VERSION_CONSUMED, version);
    }
  }

  /**
   * return the highest watermark for each partition of the given table
   * @param tableName
   * @return
   */
  public Map<Integer, Long> getHighWaterMarkForTable(String tableName) {
    return ImmutableMap.copyOf(_highWaterMarkTablePartitionMap.getOrDefault(tableName, ImmutableMap.of()));
  }

  @Override
  public void init(Configuration config, GrigioMetrics metrics) {

  }

  public Map<String, Map<Integer, Long>> getHighWaterMarkTablePartitionMap() {
    return ImmutableMap.copyOf(_highWaterMarkTablePartitionMap);
  }

  @Override
  public String toString() {
    return Joiner.on(",").withKeyValueSeparator("=").join(_highWaterMarkTablePartitionMap);
  }
}

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
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.util.internal.ConcurrentSet;
import org.apache.commons.configuration.Configuration;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.data.manager.UpsertSegmentDataManager;
import org.apache.pinot.grigio.common.messages.LogCoordinatorMessage;
import org.apache.pinot.grigio.common.metrics.GrigioMeter;
import org.apache.pinot.grigio.common.metrics.GrigioMetrics;
import org.apache.pinot.grigio.common.metrics.GrigioTimer;
import org.apache.pinot.grigio.common.rpcQueue.QueueConsumer;
import org.apache.pinot.grigio.common.rpcQueue.QueueConsumerRecord;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogEntry;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogStorageProvider;
import org.apache.pinot.grigio.common.storageProvider.retentionManager.UpdateLogRetentionManager;
import org.apache.pinot.grigio.common.utils.CommonUtils;
import org.apache.pinot.grigio.servers.SegmentUpdaterProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * class to perform fetching updates for upsert related information from kafka to local machine and fill in
 * virtual columns. It should be started after all segments are loaded in current pinot server.
 */
@ThreadSafe
public class SegmentUpdater implements SegmentDeletionListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentUpdater.class);

  private static volatile SegmentUpdater _instance = null;

  private static final long NO_MESSAGE_SLEEP_MS = 100;
  private static final long SHUTDOWN_WAIT_MS = 2000;
  private static final long LOGGER_TIME_GAP_MS = 5000;

  private final Configuration _conf;
  private final int _updateSleepMs;
  private final String _topicPrefix;
  private final ExecutorService _ingestionExecutorService;
  private final QueueConsumer _consumer;
  private final Map<String, Map<String, Set<UpsertSegmentDataManager>>> _tableSegmentMap = new ConcurrentHashMap<>();
  private final Map<String, Map<Integer, Long>> _tablePartitionCreationTime = new ConcurrentHashMap<>();
  private final UpdateLogStorageProvider _updateLogStorageProvider;
  private final UpdateLogRetentionManager _retentionManager;
  private final GrigioMetrics _metrics;

  private volatile boolean isStarted = true;

  public SegmentUpdater(Configuration conf, SegmentUpdaterProvider provider, UpdateLogRetentionManager retentionManager,
                        GrigioMetrics metrics) {
    _conf = conf;
    _metrics = metrics;
    _retentionManager = retentionManager;
    _topicPrefix = conf.getString(SegmentUpdaterConfig.INPUT_TOPIC_PREFIX);
    _updateSleepMs = conf.getInt(SegmentUpdaterConfig.SEGMENT_UDPATE_SLEEP_MS,
        SegmentUpdaterConfig.SEGMENT_UDPATE_SLEEP_MS_DEFAULT);
    UpsertWaterMarkManager.init(metrics);
    _consumer = provider.getConsumer();
    _ingestionExecutorService = Executors.newFixedThreadPool(1);
    _updateLogStorageProvider = UpdateLogStorageProvider.getInstance();
    _instance = this;
  }

  public static SegmentUpdater getInstance() {
    Preconditions.checkState(_instance != null, "there is no instance for segment updater");
    return _instance;
  }


  public void start() {
    String listOfTables = Joiner.on(",").join(_tableSegmentMap.keySet());
    LOGGER.info("starting segment updater main loop with the following table in server: {}", listOfTables);
    _ingestionExecutorService.submit(this::updateLoop);
  }

  public void shutdown() {
    LOGGER.info("closing the segment updater");
    isStarted = false;
    _ingestionExecutorService.shutdown();
    try {
      _ingestionExecutorService.awaitTermination(SHUTDOWN_WAIT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOGGER.error("failed to wait for shutdown", e);
    }
    _ingestionExecutorService.shutdownNow();
    try {
      _updateLogStorageProvider.close();
    } catch (IOException ex) {
      LOGGER.error("failed to shutdown update log provider", ex);
    }
    LOGGER.info("finished shutdown of segment updater service");
  }

  /**
   * this method will perform the following:
   * 1. fetch a list of updates for pinot upsert from kafka consumer
   * 2. organize the message by table/segment/List<UpdateLogEntry> map
   * 3. call to save the update data in local file system
   * 4. apply updates to appropriate data manager
   * 5. commit consumer offset
   */
  private void updateLoop() {
    try {
      LOGGER.info("starting update loop");
      long lastReportedTime = System.currentTimeMillis();
      while (isStarted) {
        long startTime = System.currentTimeMillis();
        long loopStartTime = startTime;
        final List<QueueConsumerRecord<String, LogCoordinatorMessage>> records = _consumer.getRequests(_updateSleepMs, TimeUnit.MILLISECONDS);
        _metrics.addTimedValueMs(GrigioTimer.FETCH_MSG_FROM_CONSUMER_TIME, System.currentTimeMillis() - startTime);
        int eventCount = records.size();
        _metrics.addMeteredGlobalValue(GrigioMeter.MESSAGE_FETCH_PER_ROUND, eventCount);

        final Map<String, TableUpdateLogs> tableSegmentToUpdateLogs = new HashMap<>();
        // organize the update logs by {tableName: {segmentName: {list of updatelogs}}}
        records.iterator().forEachRemaining(consumerRecord -> {
          TableUpdateLogs tableUpdateLogs = tableSegmentToUpdateLogs.computeIfAbsent(
              CommonUtils.getTableNameFromKafkaTopic(consumerRecord.getTopic(), _topicPrefix),
              t -> new TableUpdateLogs());
          tableUpdateLogs.addUpdateLogEntry(consumerRecord.getRecord(), consumerRecord.getPartition());
        });

        startTime = System.currentTimeMillis();
        AtomicLong timeToStoreUpdateLogs = new AtomicLong(0);
        for (Map.Entry<String, TableUpdateLogs> entry : tableSegmentToUpdateLogs.entrySet()) {
          String tableName = TableNameBuilder.ensureTableNameWithType(entry.getKey(), CommonConstants.Helix.TableType.REALTIME);
          int tableMessageCount = 0;
          if (_tableSegmentMap.containsKey(tableName)) {
            final Map<String, Set<UpsertSegmentDataManager>> segmentManagersMap = _tableSegmentMap.get(tableName);
            final TableUpdateLogs segment2UpdateLogsMap = entry.getValue();
            updateSegmentVirtualColumns(tableName, segmentManagersMap, segment2UpdateLogsMap, timeToStoreUpdateLogs);
          } else {
            LOGGER.warn("got messages for table {} not in this server", tableName);
          }
          _metrics.addMeteredTableValue(tableName, GrigioMeter.MESSAGE_FETCH_PER_ROUND, tableMessageCount);
        }
        _metrics.addTimedValueMs(GrigioTimer.UPDATE_LOCAL_LOG_FILE_TIME, timeToStoreUpdateLogs.get());
        _metrics.addTimedValueMs(GrigioTimer.UPDATE_DATAMANAGER_TIME, System.currentTimeMillis() - startTime);
        if (eventCount == 0) {
          Uninterruptibles.sleepUninterruptibly(NO_MESSAGE_SLEEP_MS, TimeUnit.MILLISECONDS);
        } else {
          if (System.currentTimeMillis() - lastReportedTime > LOGGER_TIME_GAP_MS) {
            lastReportedTime = System.currentTimeMillis();
            LOGGER.info("processed {} messages in {} ms", eventCount, System.currentTimeMillis() - loopStartTime);
            LOGGER.info("latest high water mark is {}", UpsertWaterMarkManager.getInstance().toString());
          }
          _consumer.ackOffset();
          _metrics.addTimedValueMs(GrigioTimer.SEGMENT_UPDATER_LOOP_TIME, System.currentTimeMillis() - loopStartTime);
        }
      }
    } catch (Exception ex) {
      LOGGER.error("failed at segment updates", ex);
    } finally {
      LOGGER.info("exiting segment update loop");
    }
  }

  private void storeUpdateLogs(String table, String segment, List<UpdateLogEntry> messages,
                               AtomicLong timeToStoreUpdateLogs) throws IOException {
    long startTime = System.currentTimeMillis();
    _updateLogStorageProvider.addDataToFile(table, segment, messages);
    timeToStoreUpdateLogs.addAndGet(System.currentTimeMillis() - startTime);
  }

  /**
   * Update the virtual columns of affected segments of a table.
   */
  private void updateSegmentVirtualColumns(String tableName, Map<String, Set<UpsertSegmentDataManager>> segmentManagersMap,
                                           TableUpdateLogs segment2UpdateLogsMap, AtomicLong timeToStoreUpdateLogs) throws IOException{
    for (Map.Entry<String, List<UpdateLogEntry>> segmentEntry : segment2UpdateLogsMap.getSegments2UpdateLog().entrySet()) {
      final String segmentNameStr = segmentEntry.getKey();
      updateVirtualColumn(tableName, segmentNameStr,
          segmentManagersMap.computeIfAbsent(segmentNameStr, sn -> new ConcurrentSet<>()),
          segment2UpdateLogsMap.get(segmentNameStr), timeToStoreUpdateLogs);
    }
  }

  /**
   * in pinot server, there could be multiple segment data managers per table/segment pair during pinot switch a segment
   * from consuming to online (mutable segment to immutable segment). In most of cases we expect only one segment manager
   * in this set of UpsertSegmentDataManager
   */
  private void updateVirtualColumn(String table, String segment, Set<UpsertSegmentDataManager> segmentDataManagers,
                                   List<UpdateLogEntry> messages, AtomicLong timeToStoreUpdateLogs) throws IOException {
    LOGGER.debug("updating segment {} with {} results for {} data managers", segment, messages.size(),
        segmentDataManagers.size());
    if (segmentDataManagers.size() > 0 || _retentionManager.getRetentionManagerForTable(table).shouldIngestForSegment(segment)) {
      storeUpdateLogs(table, segment, messages, timeToStoreUpdateLogs);
    }
    try {
      for (UpsertSegmentDataManager dataManager: segmentDataManagers) {
        dataManager.updateVirtualColumns(messages);
      }
    } catch (Exception ex) {
      LOGGER.error("failed to update virtual column for key ", ex);
    }
  }

  /**
   * called when we create a new segment data manager, associate this data manager with the given table/segment info
   * @param tableNameWithType
   * @param segmentName
   * @param dataManager the data manager for the current given table/segment combination
   */
  public synchronized void addSegmentDataManager(String tableNameWithType, LLCSegmentName segmentName, UpsertSegmentDataManager dataManager) {
    // TODO get partition assignment from
    LOGGER.info("segment updater adding table {} segment {}", tableNameWithType, segmentName.getSegmentName());
    if (!_tableSegmentMap.containsKey(tableNameWithType)) {
      synchronized (_tableSegmentMap) {
        _tableSegmentMap.put(tableNameWithType, new ConcurrentHashMap<>());
      }
      LOGGER.info("adding table {} to segment updater consumer", tableNameWithType);
      handleNewTableInServer(tableNameWithType);
    }
    _tableSegmentMap.get(tableNameWithType).computeIfAbsent(segmentName.getSegmentName(), sn -> new HashSet<>()).add(dataManager);
    synchronized (_tablePartitionCreationTime) {
      long creationTime = _tablePartitionCreationTime.computeIfAbsent(tableNameWithType, t -> new ConcurrentHashMap<>())
          .computeIfAbsent(segmentName.getPartitionId(), p -> segmentName.getCreationTimeStamp());
      _tablePartitionCreationTime.get(tableNameWithType)
          .put(segmentName.getPartitionId(), Long.max(creationTime, segmentName.getCreationTimeStamp()));
    }
  }

  public synchronized void removeSegmentDataManager(String tableNameWithType, String segmentName, UpsertSegmentDataManager toDeleteManager) {
    LOGGER.info("segment updater removing table {} segment {}", tableNameWithType, segmentName);
    Map<String, Set<UpsertSegmentDataManager>> segmentMap = _tableSegmentMap.get(tableNameWithType);
    if (segmentMap != null) {
      Set<UpsertSegmentDataManager> segmentDataManagers = segmentMap.get(segmentName);
      if (segmentDataManagers != null) {
        segmentDataManagers.remove(toDeleteManager);
        if (segmentDataManagers.size() == 0) {
          segmentMap.remove(segmentName);
        }
      }
    }
  }

  /**
   * handle how to read update logs we need to do for adding a new pinot table
   * need to do the following:
   * subscribe to table update log kafka topics
   * @param tableNameWithType the name of the table without
   */
  private void handleNewTableInServer(String tableNameWithType) {
    LOGGER.info("subscribing to new table {}", tableNameWithType);
    // init the retention manager to ensure we get the first ideal state
    _retentionManager.getRetentionManagerForTable(tableNameWithType);
    _consumer.subscribeForTable(TableNameBuilder.extractRawTableName(tableNameWithType), _topicPrefix);
  }

  /**
   * handle clean up when a table no longer has any segment in the current server
   * @param tableNameWithType
   */
  private void handleTableRemovalInServer(String tableNameWithType) {
    LOGGER.info("unsubscribe to old table {}", tableNameWithType);
    // key coordinator generate message without table name
    _consumer.unsubscribeForTable(TableNameBuilder.extractRawTableName(tableNameWithType), _topicPrefix);
  }

  @Override
  public synchronized void onSegmentDeletion(String tableNameWithType, String segmentName) {
    LOGGER.info("deleting segment virtual column from local storage for table {} segment {}", tableNameWithType, segmentName);
    Map<String, Set<UpsertSegmentDataManager>> segmentManagerMap = _tableSegmentMap.get(tableNameWithType);
    if (segmentManagerMap != null) {
      if (segmentManagerMap.containsKey(segmentName) && segmentManagerMap.get(segmentName).size() > 0) {
        LOGGER.error("trying to remove segment storage with {} segment data manager", segmentManagerMap.get(segmentName).size());
      }
      try {
        segmentManagerMap.remove(segmentName);
        _retentionManager.getRetentionManagerForTable(tableNameWithType).notifySegmentDeletion(tableNameWithType);
        _updateLogStorageProvider.removeSegment(tableNameWithType, segmentName);
      } catch (IOException e) {
        throw new RuntimeException(String.format("failed to delete table %s segment %s", tableNameWithType, segmentName), e);
      }
      if (segmentManagerMap.size() == 0) {
        _tableSegmentMap.remove(tableNameWithType);
        handleTableRemovalInServer(tableNameWithType);
      }
    } else {
      LOGGER.error("deleting a segment {}:{} from current server but don't have segment map on updater",
          tableNameWithType, segmentName);
    }
  }

  // A table's update logs grouped by segment names.
  private class TableUpdateLogs {
    private Map<String, List<UpdateLogEntry>> _segments2UpdateLog;

    public TableUpdateLogs() {
                                 _segments2UpdateLog = new HashMap<>();
                                                                       }

    public Map<String, List<UpdateLogEntry>> getSegments2UpdateLog() {
      return Collections.unmodifiableMap(_segments2UpdateLog);
    }

    // Partition is the partition where the record appears in the Segment Update Event message queue.
    public void addUpdateLogEntry(LogCoordinatorMessage record, int partition) {
      if (record == null || record.getSegmentName() == null) {
        LOGGER.error("Empty update log or no segment in the entry: {}", record);
        return;
      }
      _segments2UpdateLog.computeIfAbsent(
              record.getSegmentName(), s -> new ArrayList<>()).add(new UpdateLogEntry(record, partition));
    }

    public List<UpdateLogEntry> get(String segmentNameStr) {
      return Collections.unmodifiableList(_segments2UpdateLog.get(segmentNameStr));
    }
  }
}
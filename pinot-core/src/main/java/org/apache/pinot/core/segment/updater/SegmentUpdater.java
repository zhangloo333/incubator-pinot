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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.data.manager.UpsertSegmentDataManager;
import org.apache.pinot.opal.common.StorageProvider.UpdateLogEntry;
import org.apache.pinot.opal.common.StorageProvider.UpdateLogStorageProvider;
import org.apache.pinot.opal.common.messages.LogCoordinatorMessage;
import org.apache.pinot.opal.distributed.keyCoordinator.common.DistributedCommonUtils;
import org.apache.pinot.opal.distributed.keyCoordinator.serverUpdater.SegmentUpdaterProvider;
import org.apache.pinot.opal.distributed.keyCoordinator.serverUpdater.SegmentUpdateQueueConsumer;
import io.netty.util.internal.ConcurrentSet;
import org.apache.commons.configuration.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * class to perform fetching updates for upsert related information from kafka to local machine and fill in
 * virtual columns. It should be started after all segment are loaded in current pinot server
 */
public class SegmentUpdater implements SegmentDeletionListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentUpdater.class);

  private static volatile SegmentUpdater _instance = null;

  private static final long NO_MESSAGE_SLEEP_MS = 100;
  private static final long SHUTDOWN_WAIT_MS = 2000;

  private final Configuration _conf;
  private final int _updateSleepMs;
  private final ExecutorService _ingestionExecutorService;
  private final SegmentUpdateQueueConsumer _consumer;
  private final Map<String, Map<String, Set<UpsertSegmentDataManager>>> _tableSegmentMap = new ConcurrentHashMap<>();
  private final Map<String, Map<Integer, Long>> _tablePartitionCreationTime = new ConcurrentHashMap<>();
  private final UpdateLogStorageProvider _updateLogStorageProvider;

  private volatile boolean isStarted = true;

  public SegmentUpdater(Configuration conf, SegmentUpdaterProvider provider) {
    _conf = conf;
    _updateSleepMs = conf.getInt(SegmentUpdaterConfig.SEGMENT_UDPATE_SLEEP_MS,
        SegmentUpdaterConfig.SEGMENT_UDPATE_SLEEP_MS_DEFAULT);
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
    LOGGER.info("starting segment updater main loop");
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
    LOGGER.info("finished shutdown of segment updater service");
  }

  /**
   * the following method will perform the following:
   * 1. fetch a list of updates for pinot upsert from kafka consumer
   * 2. organize the message by table/segment/List<UpdateLogEntry> map
   * 3. call to save the update data in local file system
   * 4. apply updates to appropriate data manager
   * 5. commit consumer offset
   */
  private void updateLoop() {
    try {
      LOGGER.info("starting update loop");
      while(isStarted) {
        final ConsumerRecords<String, LogCoordinatorMessage> records = _consumer.getConsumerRecords(_updateSleepMs, TimeUnit.MILLISECONDS);
        final Map<String, Map<String, List<UpdateLogEntry>>> tableSegmentToUpdateLogs = new HashMap<>();
        int eventCount = records.count();

        // organize the update logs by {tableName: {segmentName: {list of updatelogs}}}
        records.iterator().forEachRemaining(consumerRecord -> {
          Map<String, List<UpdateLogEntry>> segmentMap = tableSegmentToUpdateLogs.computeIfAbsent(
              DistributedCommonUtils.getTableNameFromKafkaTopic(consumerRecord.topic()),
              t -> new HashMap<>());
          String segmentName = consumerRecord.value().getSegmentName();
          segmentMap.computeIfAbsent(segmentName, s -> new ArrayList<>()).add(new UpdateLogEntry(consumerRecord.value()));
        });

        for (Map.Entry<String, Map<String, List<UpdateLogEntry>>> entry: tableSegmentToUpdateLogs.entrySet()) {
          String tableName = entry.getKey();
          if (_tableSegmentMap.containsKey(tableName)) {
            final Map<String, Set<UpsertSegmentDataManager>> segmentManagersMap = _tableSegmentMap.get(tableName);
            final Map<String, List<UpdateLogEntry>> segmentDataMap = entry.getValue();
            for (Map.Entry<String, List<UpdateLogEntry>> segmentEntry: entry.getValue().entrySet()) {
              final String segmentNameStr = segmentEntry.getKey();
              updateVirtualColumn(tableName, segmentEntry.getKey(),
                  segmentManagersMap.computeIfAbsent(segmentNameStr, sn -> new ConcurrentSet<>()), segmentDataMap.get(segmentNameStr));
            }
          } else {
//            LOGGER.warn("got messages for table {} not in this server", tableName);
          }
        }
        if (eventCount == 0) {
          Uninterruptibles.sleepUninterruptibly(NO_MESSAGE_SLEEP_MS, TimeUnit.MILLISECONDS);
        } else {
          LOGGER.info("latest high water mark is {}", UpsertWaterMarkManager.getInstance().toString());
          _consumer.ackOffset();
        }
      }
    } catch (Exception ex) {
      LOGGER.error("failed at segment updates", ex);
    } finally {
      LOGGER.info("exiting segment update loop");
    }
    LOGGER.info("segment update failed");
  }

  /**
   * in pinot server, there could be multiple segment data managers per table/segment pair during pinot switch a segment
   * from consuming to online (mutable segment to immutable segment). In most of cases we expect only one segment manager
   * in this set of UpsertSegmentDataManager
   */
  private void updateVirtualColumn(String table, String segment, Set<UpsertSegmentDataManager> segmentDataManagers,
                                   List<UpdateLogEntry> messages) throws IOException {
    LOGGER.info("updating segment {} with {} results for {} data managers", segment, messages.size(),
        segmentDataManagers.size());

    // update storage
    _updateLogStorageProvider.addDataToFile(table, segment, messages);
    try {
      for (UpsertSegmentDataManager dataManager: segmentDataManagers) {
        dataManager.updateVirtualColumn(messages);
      }
    } catch (Exception ex) {
      LOGGER.error("failed to update virtual column for key ", ex);
    }
  }

  /**
   * called when we create a new segment data manager, associate this data manager with the given table/segment info
   * @param tableName
   * @param segmentName
   * @param dataManager the data manager for the current given table/segment combination
   */
  public synchronized void addSegmentDataManager(String tableName, LLCSegmentName segmentName, UpsertSegmentDataManager dataManager) {
    // TODO get partition assignment from
    LOGGER.info("segment updater adding table {} segment {}", tableName, segmentName.getSegmentName());
    if (!_tableSegmentMap.containsKey(tableName)) {
      synchronized (_tableSegmentMap) {
        _tableSegmentMap.put(tableName, new ConcurrentHashMap<>());
      }
    }
    _tableSegmentMap.get(tableName).computeIfAbsent(segmentName.getSegmentName(), sn -> new HashSet<>()).add(dataManager);
    synchronized (_tablePartitionCreationTime) {
      long creationTime = _tablePartitionCreationTime.computeIfAbsent(tableName, t -> new ConcurrentHashMap<>())
          .computeIfAbsent(segmentName.getPartitionId(), p -> segmentName.getCreationTimeStamp());
      _tablePartitionCreationTime.get(tableName)
          .put(segmentName.getPartitionId(), Long.max(creationTime, segmentName.getCreationTimeStamp()));
    }
  }

  public synchronized void removeSegmentDataManager(String tableName, String segmentName, UpsertSegmentDataManager toDeleteManager) {
    LOGGER.info("segment updater removing table {} segment {}", tableName, segmentName);
    Map<String, Set<UpsertSegmentDataManager>> segmentMap = _tableSegmentMap.get(tableName);
    if (segmentMap != null) {
      Set<UpsertSegmentDataManager> segmentDataManagers = segmentMap.get(segmentName);
      if (segmentDataManagers != null) {
        segmentDataManagers.remove(toDeleteManager);
        if (segmentDataManagers.size() == 0) {
          segmentMap.remove(segmentName);
        }
        if (segmentMap.size() == 0) {
          _tableSegmentMap.remove(tableName);
          // TODO do other handling of table mapping removal
        }
      }
    }
  }

  @Override
  public synchronized void onSegmentDeletion(String tableName, String segmentName) {
    LOGGER.info("deleting segment virtual column from local storage for table {} segment {}", tableName, segmentName);
    Map<String, Set<UpsertSegmentDataManager>> segmentManagerMap = _tableSegmentMap.get(tableName);
    if (segmentManagerMap != null) {
      if (segmentManagerMap.containsKey(segmentName)) {
        LOGGER.warn("trying to remove segment storage with {} segment data manager", segmentManagerMap.get(segmentName).size());
        try {
          _updateLogStorageProvider.removeSegment(tableName, segmentName);
        } catch (IOException e) {
          throw new RuntimeException(String.format("failed to delete table %s segment %s", tableName, segmentName), e);
        }
      }
    }
  }
}

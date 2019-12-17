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
package org.apache.pinot.grigio.keyCoordinator.internal;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration.Configuration;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.grigio.common.DistributedCommonUtils;
import org.apache.pinot.grigio.common.keyValueStore.ByteArrayWrapper;
import org.apache.pinot.grigio.common.keyValueStore.KeyValueStoreDB;
import org.apache.pinot.grigio.common.keyValueStore.KeyValueStoreTable;
import org.apache.pinot.grigio.common.keyValueStore.RocksDBKeyValueStoreDB;
import org.apache.pinot.grigio.common.messages.KeyCoordinatorMessageContext;
import org.apache.pinot.grigio.common.messages.KeyCoordinatorQueueMsg;
import org.apache.pinot.grigio.common.messages.LogCoordinatorMessage;
import org.apache.pinot.grigio.common.messages.LogEventType;
import org.apache.pinot.grigio.common.metrics.GrigioGauge;
import org.apache.pinot.grigio.common.metrics.GrigioMeter;
import org.apache.pinot.grigio.common.metrics.GrigioTimer;
import org.apache.pinot.grigio.common.rpcQueue.LogCoordinatorQueueProducer;
import org.apache.pinot.grigio.common.rpcQueue.ProduceTask;
import org.apache.pinot.grigio.common.rpcQueue.QueueConsumerRecord;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogEntry;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogStorageProvider;
import org.apache.pinot.grigio.common.storageProvider.retentionManager.UpdateLogRetentionManager;
import org.apache.pinot.grigio.common.storageProvider.retentionManager.UpdateLogTableRetentionManager;
import org.apache.pinot.grigio.common.updateStrategy.MessageResolveStrategy;
import org.apache.pinot.grigio.keyCoordinator.GrigioKeyCoordinatorMetrics;
import org.apache.pinot.grigio.keyCoordinator.helix.State;
import org.apache.pinot.grigio.keyCoordinator.starter.KeyCoordinatorConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * process a list of input messages for a key coordinator, sending out updates events and update internal data storage
 */
public class SegmentEventProcessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentEventProcessor.class);

  private static final long TERMINATION_WAIT_MS = 10_000L;
  public static final String PROCESS_THREAD_COUNT = "kc.processor.threadCount";
  public static final int PROCESS_THREAD_COUNT_DEFAULT = 5;

  // constructor provided param
  protected KeyCoordinatorConf _conf;
  protected LogCoordinatorQueueProducer _outputKafkaProducer;
  protected MessageResolveStrategy _messageResolveStrategy;
  protected KeyValueStoreDB<ByteArrayWrapper, KeyCoordinatorMessageContext> _keyValueStoreDB;
  protected UpdateLogStorageProvider _storageProvider;
  protected UpdateLogRetentionManager _retentionManager;
  protected GrigioKeyCoordinatorMetrics _metrics;
  protected VersionMessageManager _versionMessageManager;

  // config provided param
  protected String _topicPrefix;
  protected ExecutorService _service;

  protected volatile State _state;

  public SegmentEventProcessor(KeyCoordinatorConf conf, LogCoordinatorQueueProducer keyCoordinatorProducer,
                               MessageResolveStrategy messageResolveStrategy,
                               UpdateLogRetentionManager updateLogRetentionManager,
                               VersionMessageManager versionMessageManager,
                               GrigioKeyCoordinatorMetrics metrics) {
    this(conf, keyCoordinatorProducer, messageResolveStrategy,
        getKeyValueStore(conf.subset(KeyCoordinatorConf.KEY_COORDINATOR_KV_STORE)),
        UpdateLogStorageProvider.getInstance(),
        updateLogRetentionManager,
        versionMessageManager,
        metrics);
  }

  @VisibleForTesting
  public SegmentEventProcessor(KeyCoordinatorConf conf, LogCoordinatorQueueProducer keyCoordinatorProducer,
                               MessageResolveStrategy messageResolveStrategy,
                               KeyValueStoreDB<ByteArrayWrapper, KeyCoordinatorMessageContext> keyValueStoreDB,
                               UpdateLogStorageProvider storageProvider,
                               UpdateLogRetentionManager updateLogRetentionManager,
                               VersionMessageManager versionMessageManager,
                               GrigioKeyCoordinatorMetrics metrics) {
    _conf = conf;
    _outputKafkaProducer = keyCoordinatorProducer;
    _messageResolveStrategy = messageResolveStrategy;
    _keyValueStoreDB = keyValueStoreDB;
    _storageProvider = storageProvider;
    _retentionManager = updateLogRetentionManager;
    _versionMessageManager = versionMessageManager;
    _metrics = metrics;
    // get config
    _topicPrefix = conf.getTopicPrefix();
    _service = Executors.newFixedThreadPool(conf.getInt(PROCESS_THREAD_COUNT, PROCESS_THREAD_COUNT_DEFAULT));

    _state = State.INIT;
  }

  public void start() {
    _state = State.RUNNING;
  }

  public void stop() {
    _state = State.SHUTTING_DOWN;
    _service.shutdown();
    try  {
      _service.awaitTermination(TERMINATION_WAIT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      LOGGER.error("failed to wait for message processing thread to shutdown", ex);
    }
    _service.shutdownNow();
    _state = State.SHUTDOWN;
  }

  /**
   * process a list of update logs (update kv, send to downstream kafka, etc)
   * @param messages list of the segment ingestion event for us to use in updates
   */
  public void processMessages(List<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> messages) {
    long start = System.currentTimeMillis();
    Map<String, List<MessageWithPartitionAndVersion>> tableMsgMap = new HashMap<>();
    for (QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg> msg: messages) {
      if (msg.getRecord().isVersionMessage()) {
        // update _currentVersionConsumed for each version message
        _versionMessageManager.maybeUpdateVersionConsumed(msg.getPartition(), msg.getRecord().getVersion());
        _metrics.setValueOfTableGauge(String.valueOf(msg.getPartition()), GrigioGauge.KC_VERSION_CONSUMED, msg.getRecord().getVersion());
      } else {
        // filter out version messages, attach the current version (and partition) to each regular messages
        tableMsgMap.computeIfAbsent(msg.getRecord().getPinotTableName(), t -> new ArrayList<>()).add(
            new MessageWithPartitionAndVersion(
                msg.getRecord().getKey(),
                msg.getRecord().getSegmentName(),
                msg.getRecord().getKafkaOffset(),
                msg.getRecord().getTimestamp(),
                msg.getPartition(),
                _versionMessageManager.getVersionConsumed(msg.getPartition())
            )
        );
      }
    }
    for (Map.Entry<String, List<MessageWithPartitionAndVersion>> perTableUpdates: tableMsgMap.entrySet()) {
      processMessagesForTable(perTableUpdates.getKey(), perTableUpdates.getValue());
    }
    _metrics.addTimedValueMs(GrigioTimer.MESSAGE_PROCESS_THREAD_PROCESS_DELAY, System.currentTimeMillis() - start);
  }

  /**
   * process update for a specific table
   * @param tableName the name of the table we are processing (with type)
   * @param msgList the list of message associated with it
   */
  protected void processMessagesForTable(String tableName, List<MessageWithPartitionAndVersion> msgList) {
    Preconditions.checkState(_state == State.RUNNING, "segment update processor is not running");
    try {
      long start = System.currentTimeMillis();
      if (msgList == null || msgList.size() == 0) {
        LOGGER.warn("trying to process topic message with empty list {}", tableName);
        return;
      }
      List<ProduceTask<Integer, LogCoordinatorMessage>> tasks = new ArrayList<>();
      Map<ByteArrayWrapper, KeyCoordinatorMessageContext> primaryKeyToValueMap = fetchDataFromKVStore(tableName, msgList);
      processMessageUpdates(tableName, msgList, primaryKeyToValueMap, tasks);
      List<ProduceTask<Integer, LogCoordinatorMessage>> failedTasks = sendMessagesToLogCoordinator(tasks, 10, TimeUnit.SECONDS);
      if (failedTasks.size() > 0) {
        LOGGER.error("send to producer failed: {}", failedTasks.size());
      }
      storeMessageToLocal(tableName, tasks);
      storeMessageToKVStore(tableName, primaryKeyToValueMap);
      _metrics.addTimedTableValueMs(tableName, GrigioTimer.MESSAGE_PROCESS_THREAD_PROCESS_DELAY, System.currentTimeMillis() - start);
    } catch (IOException e) {
      throw new RuntimeException("failed to interact with rocksdb", e);
    } catch (RuntimeException e) {
      throw new RuntimeException("failed to interact with key value store", e);
    }
  }

  /**
   * create a message to be sent to output kafka topic based on a list of metadata
   * partition is the partition number of the ingestion upsert event. Note that the partition of records with primary
   * key will be the same across ingestion upsert event and segment update event topics.
   * TODO: remove name referring log coordinator
   */
  protected ProduceTask<Integer, LogCoordinatorMessage> createMessageToLogCoordinator(String tableName, String segmentName,
                                                                                    long oldKafkaOffset, long value,
                                                                                    LogEventType eventType, int partition) {
    return new ProduceTask<>(DistributedCommonUtils.getKafkaTopicFromTableName(tableName, _topicPrefix),
        partition, new LogCoordinatorMessage(segmentName, oldKafkaOffset, value, eventType));
  }

  /**
   * fetch all available data from kv from the primary key associated with the messages in the given message list
   * @param tableName the name of table we are processing
   * @param msgList list of ingestion update messages
   * @return map of primary key and their associated state in key-value store
   * @throws IOException
   */
  protected Map<ByteArrayWrapper, KeyCoordinatorMessageContext> fetchDataFromKVStore(String tableName,
                                                                                   List<MessageWithPartitionAndVersion> msgList)
      throws IOException {
    long start = System.currentTimeMillis();
    KeyValueStoreTable<ByteArrayWrapper, KeyCoordinatorMessageContext> table = _keyValueStoreDB.getTable(tableName);
    // get a set of unique primary key and retrieve its corresponding value in kv store
    Set<ByteArrayWrapper> primaryKeys = msgList.stream()
        .filter(msg -> msg.getKey() != null && msg.getKey().length > 0)
        .map(msg -> new ByteArrayWrapper(msg.getKey()))
        .collect(Collectors.toCollection(HashSet::new));
    Map<ByteArrayWrapper, KeyCoordinatorMessageContext> primaryKeyToValueMap = new HashMap<>(table.multiGet(new ArrayList<>(primaryKeys)));
    _metrics.setValueOfGlobalGauge(GrigioGauge.FETCH_MSG_FROM_KV_COUNT, primaryKeyToValueMap.size());
    _metrics.addTimedValueMs(GrigioTimer.FETCH_MSG_FROM_KV_DELAY, System.currentTimeMillis() - start);
    _metrics.addTimedTableValueMs(tableName, GrigioTimer.FETCH_MSG_FROM_KV_DELAY, System.currentTimeMillis() - start);
    LOGGER.info("input keys got {} results from kv store in {} ms", primaryKeyToValueMap.size(), System.currentTimeMillis() - start);
    return primaryKeyToValueMap;
  }

  /**
   * process whether a list of messages should be treated as update based on the existing data from kv store
   * @param tableName the name of the table
   * @param msgList a list of ingestion update messages to be processed
   * @param primaryKeyToValueMap the current kv store state associated with this list of messages, will be updated to
   *                             reflect the new state after these updates are done
   * @param tasks the generated producer task to be sent to segment update entry queue (kafka topic)
   */
  protected void processMessageUpdates(String tableName, List<MessageWithPartitionAndVersion> msgList,
                                     Map<ByteArrayWrapper, KeyCoordinatorMessageContext> primaryKeyToValueMap,
                                     List<ProduceTask<Integer, LogCoordinatorMessage>> tasks) {
    // TODO: add a unit test
    long start = System.currentTimeMillis();
    UpdateLogTableRetentionManager tableRetentionManager = _retentionManager.getRetentionManagerForTable(
        TableNameBuilder.ensureTableNameWithType(tableName, CommonConstants.Helix.TableType.REALTIME));
    for (MessageWithPartitionAndVersion msg: msgList) {
      KeyCoordinatorMessageContext currentContext = msg.getContext();
      ByteArrayWrapper key = new ByteArrayWrapper(msg.getKey());
      if (primaryKeyToValueMap.containsKey(key)) {
        // key conflicts, should resolve which one to delete
        KeyCoordinatorMessageContext oldContext = primaryKeyToValueMap.get(key);
        if (oldContext.equals(currentContext)) {
          // message are equals, it is from another replica and should skip
          continue;
        }
        if (_messageResolveStrategy.shouldDeleteFirstMessage(oldContext, currentContext)) {
          // the existing message we have is older than the message we just processed, create delete for it
          if (tableRetentionManager.shouldIngestForSegment(oldContext.getSegmentName())) {
            // only generate delete event if the segment is still valid
            tasks.add(createMessageToLogCoordinator(tableName, oldContext.getSegmentName(), oldContext.getKafkaOffset(),
                msg.getVersion(), LogEventType.DELETE, msg.getPartition()));
          }
          // update the local cache to the latest message, so message within the same batch can override each other
          primaryKeyToValueMap.put(key, currentContext);
          tasks.add(createMessageToLogCoordinator(tableName, currentContext.getSegmentName(), currentContext.getKafkaOffset(),
              msg.getVersion(), LogEventType.INSERT, msg.getPartition()));
        }
      } else {
        // no key in the existing map, adding this key to the running set
        primaryKeyToValueMap.put(key, currentContext);
        tasks.add(createMessageToLogCoordinator(tableName, currentContext.getSegmentName(), currentContext.getKafkaOffset(),
            msg.getVersion(), LogEventType.INSERT, msg.getPartition()));
      }
    }
    _metrics.addTimedValueMs(GrigioTimer.PROCESS_MSG_UPDATE, System.currentTimeMillis() - start);
    LOGGER.info("processed all messages in {} ms", System.currentTimeMillis() - start);
  }


  /**
   * send the list of the message to downstream kafka topic
   * @param tasks the list of the tasks we are going to produce to downstream kafka
   * @param timeout how much time we wait for producer to send the messages
   * @param timeUnit the timeunit for waiting for the producers
   * @return a list of the tasks we failed to produce to downstream
   */
  protected List<ProduceTask<Integer, LogCoordinatorMessage>> sendMessagesToLogCoordinator(
      List<ProduceTask<Integer, LogCoordinatorMessage>> tasks, long timeout, TimeUnit timeUnit) {
    long startTime = System.currentTimeMillis();
    // send all and wait for result, batch for better perf
    CountDownLatch countDownLatch = new CountDownLatch(tasks.size());
    tasks.forEach(t -> t.setCallback(new ProduceTask.Callback() {
      @Override
      public void onSuccess() {
        countDownLatch.countDown();
      }
      @Override
      public void onFailure(Exception ex) {
        countDownLatch.countDown();
      }
    }));
    _outputKafkaProducer.batchProduce(tasks);
    _outputKafkaProducer.flush();
    try {
      countDownLatch.await(timeout, timeUnit);
      // right now we only set up a metrics for recording produce fails
      // TODO: design a better way to handle kafka failure
      List<ProduceTask<Integer, LogCoordinatorMessage>> failedOrTimeoutTasks = tasks.stream().filter(t -> !t.isSucceed()).collect(Collectors.toList());
      _metrics.addMeteredGlobalValue(GrigioMeter.MESSAGE_PRODUCE_FAILED_COUNT, failedOrTimeoutTasks.size());
      return failedOrTimeoutTasks;
    } catch (InterruptedException e) {
      throw new RuntimeException("encountered run time exception while waiting for the loop to finish");
    } finally {
      _metrics.addTimedValueMs(GrigioTimer.SEND_MSG_TO_KAFKA, System.currentTimeMillis() - startTime);
      LOGGER.info("send to producer take {} ms", System.currentTimeMillis() - startTime);
    }
  }

  protected static KeyValueStoreDB<ByteArrayWrapper, KeyCoordinatorMessageContext> getKeyValueStore(Configuration conf) {
    RocksDBKeyValueStoreDB keyValueStore = new RocksDBKeyValueStoreDB();
    keyValueStore.init(conf);
    return keyValueStore;
  }

  /**
   * store updates to local update log file, organized by segments
   * @param tableName
   * @param tasks
   * @throws IOException
   */
  protected void storeMessageToLocal(String tableName, List<ProduceTask<Integer, LogCoordinatorMessage>> tasks) throws IOException {
    String tableNameWithType = TableNameBuilder.ensureTableNameWithType(tableName, CommonConstants.Helix.TableType.REALTIME);
    long start = System.currentTimeMillis();
    Map<String, List<UpdateLogEntry>> segmentUpdateLogs = new HashMap<>();
    for (ProduceTask<Integer, LogCoordinatorMessage> task: tasks) {
      String segmentName = task.getValue().getSegmentName();
      UpdateLogEntry entry = new UpdateLogEntry(task.getValue(), task.getKey());
      segmentUpdateLogs.computeIfAbsent(segmentName, s -> new ArrayList<>()).add(entry);
    }
    for (Map.Entry<String, List<UpdateLogEntry>> segmentEntry: segmentUpdateLogs.entrySet()) {
      _storageProvider.addDataToFile(tableNameWithType, segmentEntry.getKey(), segmentEntry.getValue());
    }
    long duration = System.currentTimeMillis() - start;
    _metrics.addTimedValueMs(GrigioTimer.STORE_UPDATE_ON_DISK, duration);
    _metrics.addTimedTableValueMs(tableName, GrigioTimer.STORE_UPDATE_ON_DISK, duration);
    LOGGER.info("stored all data to files in {} ms", System.currentTimeMillis() - start);
  }

  /**
   * store updates to local kv store
   * @param tableName the name of the table
   * @param primaryKeyToValueMap the mapping between the primary-key and the their associated state
   * @throws IOException
   */
  protected void storeMessageToKVStore(String tableName, Map<ByteArrayWrapper, KeyCoordinatorMessageContext> primaryKeyToValueMap)
      throws IOException {
    long start = System.currentTimeMillis();
    // update kv store
    KeyValueStoreTable<ByteArrayWrapper, KeyCoordinatorMessageContext> table = _keyValueStoreDB.getTable(tableName);
    table.multiPut(primaryKeyToValueMap);
    _metrics.addTimedValueMs(GrigioTimer.STORE_UPDATE_ON_KV, System.currentTimeMillis() - start);
    LOGGER.info("updated {} message to key value store in {} ms", primaryKeyToValueMap.size(), System.currentTimeMillis() - start);
  }

  /**
   * Partially processed ingestion upsert messages, with partition and version added
   */
  protected static class MessageWithPartitionAndVersion {
    private final byte[] _key;
    private final String _segmentName;
    private final long _kafkaOffset;
    private final long _timestamp;
    private final int _partition;
    private final long _version;

    public MessageWithPartitionAndVersion(byte[] key, String segmentName, long kafkaOffset, long timestamp,
                                          int partition, long version) {
      _key = key;
      _segmentName = segmentName;
      _kafkaOffset = kafkaOffset;
      _timestamp = timestamp;
      _partition = partition;
      _version = version;
    }

    public byte[] getKey() {
      return _key;
    }

    public int getPartition() {
      return _partition;
    }

    public long getVersion() {
      return _version;
    }

    public KeyCoordinatorMessageContext getContext() {
      return new KeyCoordinatorMessageContext(_segmentName, _timestamp, _kafkaOffset);
    }

    @Override
    public String toString() {
      return "MessageWithPartitionAndVersion{" +
          "_key=" + Arrays.toString(_key) +
          ", _segmentName='" + _segmentName + '\'' +
          ", _kafkaOffset=" + _kafkaOffset +
          ", _timestamp=" + _timestamp +
          ", _partition=" + _partition +
          ", _version=" + _version +
          '}';
    }
  }
}

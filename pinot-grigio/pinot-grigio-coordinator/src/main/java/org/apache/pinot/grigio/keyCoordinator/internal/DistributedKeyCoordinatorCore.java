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
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.configuration.Configuration;
import org.apache.pinot.grigio.common.DistributedCommonUtils;
import org.apache.pinot.grigio.common.OffsetInfo;
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
import org.apache.pinot.grigio.common.rpcQueue.KeyCoordinatorQueueConsumer;
import org.apache.pinot.grigio.common.rpcQueue.LogCoordinatorQueueProducer;
import org.apache.pinot.grigio.common.rpcQueue.ProduceTask;
import org.apache.pinot.grigio.common.rpcQueue.QueueConsumerRecord;
import org.apache.pinot.grigio.common.rpcQueue.VersionMsgQueueProducer;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogEntry;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogStorageProvider;
import org.apache.pinot.grigio.common.updateStrategy.MessageResolveStrategy;
import org.apache.pinot.grigio.common.utils.CommonUtils;
import org.apache.pinot.grigio.common.utils.PartitionIdMapper;
import org.apache.pinot.grigio.keyCoordinator.GrigioKeyCoordinatorMetrics;
import org.apache.pinot.grigio.keyCoordinator.helix.KeyCoordinatorClusterHelixManager;
import org.apache.pinot.grigio.keyCoordinator.helix.KeyCoordinatorLeadershipManager;
import org.apache.pinot.grigio.keyCoordinator.helix.KeyCoordinatorVersionManager;
import org.apache.pinot.grigio.keyCoordinator.helix.State;
import org.apache.pinot.grigio.keyCoordinator.starter.KeyCoordinatorConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class DistributedKeyCoordinatorCore {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedKeyCoordinatorCore.class);
  private static final long TERMINATION_WAIT_MS = 10000;

  protected KeyCoordinatorConf _conf;
  protected LogCoordinatorQueueProducer _outputKafkaProducer;
  protected KeyCoordinatorQueueConsumer _inputKafkaConsumer;
  protected VersionMsgQueueProducer _versionMessageKafkaProducer;
  protected MessageResolveStrategy _messageResolveStrategy;
  protected KeyValueStoreDB<ByteArrayWrapper, KeyCoordinatorMessageContext> _keyValueStoreDB;
  protected ExecutorService _messageProcessThread;
  protected ExecutorService _consumerThread;
  protected BlockingQueue<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> _consumerRecordBlockingQueue;
  protected PartitionIdMapper _partitionIdMapper;
  protected UpdateLogStorageProvider _storageProvider;
  protected KeyCoordinatorClusterHelixManager _keyCoordinatorClusterHelixManager;
  protected KeyCoordinatorLeadershipManager _keyCoordinatorLeadershipManager;
  protected KeyCoordinatorVersionManager _keyCoordinatorVersionManager;
  protected GrigioKeyCoordinatorMetrics _metrics;
  protected int _fetchMsgDelayMs;
  protected int _fetchMsgMaxDelayMs;
  protected int _fetchMsgMaxCount;
  protected long _versionMessageIntervalMs;
  protected Timer _versionMessageTimer;
  protected TimerTask _versionMessageProduceTask;
  protected Map<Integer, Long> _currentVersionConsumed;

  protected volatile State _state = State.SHUTDOWN;

  public DistributedKeyCoordinatorCore() {}

  public void init(KeyCoordinatorConf conf, LogCoordinatorQueueProducer keyCoordinatorProducer,
                   KeyCoordinatorQueueConsumer keyCoordinatorConsumer, VersionMsgQueueProducer versionMessageKafkaProducer,
                   MessageResolveStrategy messageResolveStrategy,
                   KeyCoordinatorClusterHelixManager keyCoordinatorClusterHelixManager, GrigioKeyCoordinatorMetrics metrics) {
    init(conf, keyCoordinatorProducer, keyCoordinatorConsumer, versionMessageKafkaProducer, messageResolveStrategy,
        getKeyValueStore(conf.subset(KeyCoordinatorConf.KEY_COORDINATOR_KV_STORE)), Executors.newSingleThreadExecutor(),
        Executors.newSingleThreadExecutor(), UpdateLogStorageProvider.getInstance(), keyCoordinatorClusterHelixManager, metrics);
  }

  @VisibleForTesting
  public void init(KeyCoordinatorConf conf, LogCoordinatorQueueProducer keyCoordinatorProducer,
                   KeyCoordinatorQueueConsumer keyCoordinatorConsumer,
                   VersionMsgQueueProducer versionMessageKafkaProducer,
                   MessageResolveStrategy messageResolveStrategy,
                   KeyValueStoreDB<ByteArrayWrapper, KeyCoordinatorMessageContext> keyValueStoreDB,
                   ExecutorService coreThread, ExecutorService consumerThread, UpdateLogStorageProvider storageProvider,
                   KeyCoordinatorClusterHelixManager keyCoordinatorClusterHelixManager, GrigioKeyCoordinatorMetrics metrics) {
    CommonUtils.printConfiguration(conf, "distributed key coordinator core");
    Preconditions.checkState(_state == State.SHUTDOWN, "can only init if it is not running yet");
    _conf = conf;
    _outputKafkaProducer = keyCoordinatorProducer;
    _inputKafkaConsumer = keyCoordinatorConsumer;
    _versionMessageKafkaProducer = versionMessageKafkaProducer;
    _messageResolveStrategy = messageResolveStrategy;
    _keyValueStoreDB = keyValueStoreDB;
    _messageProcessThread = coreThread;
    _partitionIdMapper = new PartitionIdMapper();
    _consumerThread = consumerThread;
    _consumerRecordBlockingQueue = new ArrayBlockingQueue<>(_conf.getConsumerBlockingQueueSize());
    _storageProvider = storageProvider;
    _keyCoordinatorClusterHelixManager = keyCoordinatorClusterHelixManager;
    _metrics = metrics;

    _fetchMsgDelayMs = conf.getInt(KeyCoordinatorConf.FETCH_MSG_DELAY_MS,
        KeyCoordinatorConf.FETCH_MSG_DELAY_MS_DEFAULT);
    _fetchMsgMaxDelayMs = conf.getInt(KeyCoordinatorConf.FETCH_MSG_MAX_DELAY_MS,
        KeyCoordinatorConf.FETCH_MSG_MAX_DELAY_MS_DEFAULT);
    _fetchMsgMaxCount = conf.getInt(KeyCoordinatorConf.FETCH_MSG_MAX_BATCH_SIZE,
        KeyCoordinatorConf.FETCH_MSG_MAX_BATCH_SIZE_DEFAULT);
    LOGGER.info("starting with fetch delay: {} max delay: {}, fetch max count: {}", _fetchMsgDelayMs, _fetchMsgMaxDelayMs,
        _fetchMsgMaxCount);

    _versionMessageIntervalMs = conf.getLong(KeyCoordinatorConf.VERSION_MESSAGE_INTERVAL_MS,
        KeyCoordinatorConf.VERSION_MESSAGE_INTERVAL_MS_DEFAULT);
    _versionMessageTimer = new Timer();
    _versionMessageProduceTask = new TimerTask() {
      @Override
      public void run() {
        produceVersionMessage();
      }
    };
    _keyCoordinatorLeadershipManager =
        new KeyCoordinatorLeadershipManager(_keyCoordinatorClusterHelixManager.getControllerHelixManager());
    _keyCoordinatorVersionManager =
        new KeyCoordinatorVersionManager(_keyCoordinatorClusterHelixManager.getControllerHelixManager());

    _currentVersionConsumed = _keyCoordinatorVersionManager.getVersionConsumedFromPropertyStore();
    _state = State.INIT;
  }

  public void start() {
    Preconditions.checkState(_state == State.INIT, "key coordinate is not in correct state");
    LOGGER.info("starting key coordinator message process loop");
    _state = State.RUNNING;
    _versionMessageTimer.schedule(_versionMessageProduceTask, 0L, _versionMessageIntervalMs);
    _consumerThread.submit(this::consumerIngestLoop);
    _messageProcessThread.submit(this::messageProcessLoop);
  }

  private void produceVersionMessage() {
    long start = System.currentTimeMillis();
    if (_state != State.RUNNING) {
      LOGGER.info("Key coordinator not running, skip producing version messages");
      return;
    }
    if (!_keyCoordinatorLeadershipManager.isLeader()) {
      LOGGER.debug("Not controller leader, skip producing version messages");
      return;
    }
    try {
      long versionProduced = _keyCoordinatorVersionManager.getVersionProducedFromPropertyStore();
      long versionToProduce = versionProduced + 1;
      // produce to all partitions
      for (int partition = 0; partition < _conf.getKeyCoordinatorMessagePartitionCount(); partition++) {
        ProduceTask<Integer, KeyCoordinatorQueueMsg> produceTask =
            new ProduceTask<>(_conf.getKeyCoordinatorMessageTopic(), partition,
                new KeyCoordinatorQueueMsg(versionToProduce));
        _versionMessageKafkaProducer.produce(produceTask);
      }
      // todo: make producing version messages and setting versions to property store as one transaction
      _keyCoordinatorVersionManager.setVersionProducedToPropertyStore(versionToProduce);
      long duration = System.currentTimeMillis() - start;
      _metrics.addTimedValueMs(GrigioTimer.PRODUCE_VERSION_MESSAGE, duration);
      _metrics.setValueOfGlobalGauge(GrigioGauge.VERSION_PRODUCED, versionToProduce);
      LOGGER.info("Produced version messages to all partitions with version {} in {} ms", versionToProduce, duration);
    } catch (Exception ex) {
      LOGGER.error("Failed to produce version message", ex);
    }
  }

  private void consumerIngestLoop() {
    while (_state == State.RUNNING) {
      try {
        _metrics.setValueOfGlobalGauge(GrigioGauge.MESSAGE_PROCESS_QUEUE_SIZE, _consumerRecordBlockingQueue.size());
        List<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> records = _inputKafkaConsumer.getRequests(_fetchMsgMaxDelayMs,
            TimeUnit.MILLISECONDS);
        if (records.size() == 0) {
          LOGGER.info("no message found in kafka consumer, sleep and wait for next batch");
          Uninterruptibles.sleepUninterruptibly(_fetchMsgMaxDelayMs, TimeUnit.MILLISECONDS);
        } else {
          records.forEach(c -> {
            try {
              _consumerRecordBlockingQueue.put(c);
            } catch (InterruptedException e) {
              LOGGER.warn("exception while trying to put message to queue", e);
            }
          });
        }
      } catch (Exception ex) {
        LOGGER.error("encountered exception in consumer ingest loop, will retry", ex);
      }
    }
    LOGGER.info("exiting consumer ingest loop");
  }

  private void messageProcessLoop() {
    try {
      long deadline = System.currentTimeMillis() + _fetchMsgMaxDelayMs;
      while (_state == State.RUNNING) {
        LOGGER.info("starting new loop");
        long start = System.currentTimeMillis();
        // process message when we got max message count or reach max delay ms
        MessageAndOffset<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> messageAndOffset = getMessagesFromQueue(_consumerRecordBlockingQueue, deadline);
        List<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> messages = messageAndOffset.getMessages();
        deadline = System.currentTimeMillis() + _fetchMsgMaxDelayMs;

        _metrics.addMeteredGlobalValue(GrigioMeter.MESSAGE_PROCESS_THREAD_FETCH_COUNT, messages.size());
        _metrics.addTimedValueMs(GrigioTimer.MESSAGE_PROCESS_THREAD_FETCH_DELAY, System.currentTimeMillis() - start);

        if (messages.size() > 0) {
          processMessages(messages);
          // todo: make ackOffset and setVersionConsumed as one transaction
          _inputKafkaConsumer.ackOffset(messageAndOffset.getOffsetInfo());
          _keyCoordinatorVersionManager.setVersionConsumedToPropertyStore(_currentVersionConsumed);
          LOGGER.info("kc message processed {} messages in this loop for {} ms", messages.size(),
              System.currentTimeMillis() - start);
        } else {
          LOGGER.info("no message received in the current loop");
        }
      }
    } catch (Exception ex) {
      LOGGER.warn("key coordinator is exiting due to exception", ex);
    } finally {
      _state = State.SHUTTING_DOWN;
      LOGGER.info("exiting key coordinator loop");
    }
    LOGGER.info("existing key coordinator core /procthread");
  }

  /**
   * get a list of messages read by the consumer ingestion thread
   * @param queue the blocking queue shared with the ingestion thread
   * @param deadline at which time we should stop the ingestion and return it to caller with the data we have
   * @return list of messages to be processed
   */
  private MessageAndOffset<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> getMessagesFromQueue(
      BlockingQueue<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> queue, long deadline) {
    List<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> buffer = new ArrayList<>(_fetchMsgMaxCount * 2);
    while(System.currentTimeMillis() < deadline && buffer.size() < _fetchMsgMaxCount) {
      queue.drainTo(buffer, _fetchMsgMaxCount - buffer.size());
      if (buffer.size() < _fetchMsgMaxCount) {
        Uninterruptibles.sleepUninterruptibly(_fetchMsgDelayMs, TimeUnit.MILLISECONDS);
      }
    }
    OffsetInfo offsetInfo = new OffsetInfo();
    for (QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg> record: buffer) {
      offsetInfo.updateOffsetIfNecessary(record);
    }
    return new MessageAndOffset<>(buffer, offsetInfo);
  }

  public void stop() {
    _state = State.SHUTTING_DOWN;
    _messageProcessThread.shutdown();
    _consumerThread.shutdown();
    _versionMessageTimer.cancel();
    try {
      _consumerThread.awaitTermination(TERMINATION_WAIT_MS, TimeUnit.MILLISECONDS);
      _messageProcessThread.awaitTermination(TERMINATION_WAIT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      LOGGER.error("failed to wait for key coordinator thread to shutdown", ex);
    }
    _consumerThread.shutdownNow();
    _messageProcessThread.shutdownNow();
    _state = State.SHUTDOWN;
  }

  public State getState() {
    return _state;
  }

  /**
   * process a list of update logs (update kv, send to downstream kafka, etc)
   * @param messages list of the segment ingestion event for us to use in updates
   */
  private void processMessages(List<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> messages) {
    long start = System.currentTimeMillis();
    Map<String, List<MessageWithPartitionAndVersion>> tableMsgMap = new HashMap<>();
    for (QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg> msg: messages) {
      if (msg.getRecord().isVersionMessage()) {
        // update _currentVersionConsumed for each version message
        _currentVersionConsumed.put(msg.getPartition(), msg.getRecord().getVersion());
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
                _currentVersionConsumed.getOrDefault(msg.getPartition(), 0L)
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
  private void processMessagesForTable(String tableName, List<MessageWithPartitionAndVersion> msgList) {
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
   */
  private ProduceTask<Integer, LogCoordinatorMessage> createMessageToLogCoordinator(String tableName, String segmentName,
                                                                                    long oldKafkaOffset, long value,
                                                                                    LogEventType eventType, int partition) {
    return new ProduceTask<>(DistributedCommonUtils.getKafkaTopicFromTableName(tableName),
            partition, new LogCoordinatorMessage(segmentName, oldKafkaOffset, value, eventType));
  }

  /**
   * fetch all available data from kv from the primary key associated with the messages in the given message list
   * @param tableName the name of table we are processing
   * @param msgList list of ingestion update messages
   * @return map of primary key and their associated state in key-value store
   * @throws IOException
   */
  private Map<ByteArrayWrapper, KeyCoordinatorMessageContext> fetchDataFromKVStore(String tableName,
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
  private void processMessageUpdates(String tableName, List<MessageWithPartitionAndVersion> msgList,
                                     Map<ByteArrayWrapper, KeyCoordinatorMessageContext> primaryKeyToValueMap,
                                     List<ProduceTask<Integer, LogCoordinatorMessage>> tasks) {
    // TODO: add a unit test
    long start = System.currentTimeMillis();
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
          tasks.add(createMessageToLogCoordinator(tableName, oldContext.getSegmentName(), oldContext.getKafkaOffset(),
              msg.getVersion(), LogEventType.DELETE, msg.getPartition()));
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
  private List<ProduceTask<Integer, LogCoordinatorMessage>> sendMessagesToLogCoordinator(
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

  protected KeyValueStoreDB<ByteArrayWrapper, KeyCoordinatorMessageContext> getKeyValueStore(Configuration conf) {
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
  private void storeMessageToLocal(String tableName, List<ProduceTask<Integer, LogCoordinatorMessage>> tasks) throws IOException {
    long start = System.currentTimeMillis();
    Map<String, List<UpdateLogEntry>> segmentUpdateLogs = new HashMap<>();
    for (ProduceTask<Integer, LogCoordinatorMessage> task: tasks) {
      String segmentName = task.getValue().getSegmentName();
      UpdateLogEntry entry = new UpdateLogEntry(task.getValue(), task.getKey());
      segmentUpdateLogs.computeIfAbsent(segmentName, s -> new ArrayList<>()).add(entry);
    }
    for (Map.Entry<String, List<UpdateLogEntry>> segmentEntry: segmentUpdateLogs.entrySet()) {
      _storageProvider.addSegment(tableName, segmentEntry.getKey());
      _storageProvider.addDataToFile(tableName, segmentEntry.getKey(), segmentEntry.getValue());
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
  private void storeMessageToKVStore(String tableName, Map<ByteArrayWrapper, KeyCoordinatorMessageContext> primaryKeyToValueMap)
      throws IOException {
    long start = System.currentTimeMillis();
    // update kv store
    KeyValueStoreTable<ByteArrayWrapper, KeyCoordinatorMessageContext> table = _keyValueStoreDB.getTable(tableName);
    table.multiPut(primaryKeyToValueMap);
    _metrics.addTimedValueMs(GrigioTimer.STORE_UPDATE_ON_KV, System.currentTimeMillis() - start);
    LOGGER.info("updated {} message to key value store in {} ms", primaryKeyToValueMap.size(), System.currentTimeMillis() - start);
  }

  private static class MessageAndOffset<K> {
    private List<K> _messages;
    private OffsetInfo _offsetInfo;

    public MessageAndOffset(List<K> messages, OffsetInfo offsetInfo) {
      _messages = messages;
      _offsetInfo  = offsetInfo;
    }

    public List<K> getMessages() {
      return _messages;
    }

    public OffsetInfo getOffsetInfo() {
      return _offsetInfo;
    }
  }

  /**
   * Partially processed ingestion upsert messages, with partition and version added
   */
  private static class MessageWithPartitionAndVersion {
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
  }
}

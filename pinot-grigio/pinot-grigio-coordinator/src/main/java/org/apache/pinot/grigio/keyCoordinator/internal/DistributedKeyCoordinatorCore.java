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
import org.apache.pinot.grigio.common.rpcQueue.KeyCoordinatorQueueConsumer;
import org.apache.pinot.grigio.common.rpcQueue.KeyCoordinatorQueueProducer;
import org.apache.pinot.grigio.common.rpcQueue.LogCoordinatorQueueProducer;
import org.apache.pinot.grigio.common.rpcQueue.ProduceTask;
import org.apache.pinot.grigio.common.rpcQueue.QueueConsumerRecord;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogEntry;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogStorageProvider;
import org.apache.pinot.grigio.common.updateStrategy.MessageResolveStrategy;
import org.apache.pinot.grigio.common.utils.CommonUtils;
import org.apache.pinot.grigio.common.utils.PartitionIdMapper;
import org.apache.pinot.grigio.keyCoordinator.helix.KeyCoordinatorClusterHelixManager;
import org.apache.pinot.grigio.keyCoordinator.helix.KeyCoordinatorLeadershipManager;
import org.apache.pinot.grigio.keyCoordinator.helix.KeyCoordinatorVersionManager;
import org.apache.pinot.grigio.keyCoordinator.helix.State;
import org.apache.pinot.grigio.keyCoordinator.starter.KeyCoordinatorConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
  protected KeyCoordinatorQueueProducer _versionMessageKafkaProducer;
  protected MessageResolveStrategy _messageResolveStrategy;
  protected KeyValueStoreDB<ByteArrayWrapper, KeyCoordinatorMessageContext> _keyValueStoreDB;
  protected ExecutorService _messageProcessThread;
  protected ExecutorService _consumerThread;
  protected BlockingQueue<QueueConsumerRecord<Integer, KeyCoordinatorQueueMsg>> _consumerRecordBlockingQueue;
  protected PartitionIdMapper _partitionIdMapper;
  protected UpdateLogStorageProvider _storageProvider;
  protected KeyCoordinatorClusterHelixManager _keyCoordinatorClusterHelixManager;
  protected KeyCoordinatorLeadershipManager _keyCoordinatorLeadershipManager;
  protected KeyCoordinatorVersionManager _keyCoordinatorVersionManager;
  protected int _fetchMsgDelayMs;
  protected int _fetchMsgMaxDelayMs;
  protected int _fetchMsgMaxCount;
  protected long _versionMessageIntervalMs;
  protected Timer _versionMessageTimer;
  protected TimerTask _versionMessageProduceTask;
  protected List<Long> _currentVersionConsumed;

  protected volatile State _state = State.SHUTDOWN;

  public DistributedKeyCoordinatorCore() {}

  public void init(KeyCoordinatorConf conf, LogCoordinatorQueueProducer keyCoordinatorProducer,
                   KeyCoordinatorQueueConsumer keyCoordinatorConsumer, KeyCoordinatorQueueProducer versionMessageKafkaProducer,
                   MessageResolveStrategy messageResolveStrategy, KeyCoordinatorClusterHelixManager keyCoordinatorClusterHelixManager) {
    init(conf, keyCoordinatorProducer, keyCoordinatorConsumer, versionMessageKafkaProducer, messageResolveStrategy,
        getKeyValueStore(conf.subset(KeyCoordinatorConf.KEY_COORDINATOR_KV_STORE)), Executors.newSingleThreadExecutor(),
        Executors.newSingleThreadExecutor(), UpdateLogStorageProvider.getInstance(), keyCoordinatorClusterHelixManager);
  }

  @VisibleForTesting
  public void init(KeyCoordinatorConf conf, LogCoordinatorQueueProducer keyCoordinatorProducer,
                   KeyCoordinatorQueueConsumer keyCoordinatorConsumer,
                   KeyCoordinatorQueueProducer versionMessageKafkaProducer,
                   MessageResolveStrategy messageResolveStrategy,
                   KeyValueStoreDB<ByteArrayWrapper, KeyCoordinatorMessageContext> keyValueStoreDB,
                   ExecutorService coreThread, ExecutorService consumerThread, UpdateLogStorageProvider storageProvider,
                   KeyCoordinatorClusterHelixManager keyCoordinatorClusterHelixManager) {
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

    // todo: handle key coordinator restart
    _currentVersionConsumed = new ArrayList<>(Collections.nCopies(conf.getKeyCoordinatorMessagePartitionCount(), 0L));

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
    if (_state == State.RUNNING) {
      if (_keyCoordinatorLeadershipManager.isLeader()) {
        long versionProduced = _keyCoordinatorVersionManager.getVersionProducedFromPropertyStore();
        long versionToProduce = versionProduced + 1;
        LOGGER.info("Producing version messages to all partitions with version {}", versionToProduce);
        try {
          // produce to all partitions
          for (int partition = 0; partition < _conf.getKeyCoordinatorMessagePartitionCount(); partition++) {
            ProduceTask<Integer, KeyCoordinatorQueueMsg> produceTask =
                new ProduceTask<>(_conf.getKeyCoordinatorMessageTopic(), partition,
                    new KeyCoordinatorQueueMsg(versionToProduce));
            _versionMessageKafkaProducer.produce(produceTask);
          }
          // todo: make producing version messages and setting versions in property store atomic
          _keyCoordinatorVersionManager.setVersionProducedToPropertyStore(versionToProduce);
        } catch (Exception ex) {
          LOGGER.error("Failed to produce version message", ex);
        }
      } else {
        LOGGER.debug("Not controller leader, skip producing version messages");
      }
    }
  }

  private void consumerIngestLoop() {
    while (_state == State.RUNNING) {
      try {
        List<QueueConsumerRecord<Integer, KeyCoordinatorQueueMsg>> records = _inputKafkaConsumer.getRequests(_fetchMsgMaxDelayMs,
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
        MessageAndOffset<QueueConsumerRecord<Integer, KeyCoordinatorQueueMsg>> messageAndOffset = getMessagesFromQueue(_consumerRecordBlockingQueue, deadline);
        List<QueueConsumerRecord<Integer, KeyCoordinatorQueueMsg>> messages = messageAndOffset.getMessages();
        deadline = System.currentTimeMillis() + _fetchMsgMaxDelayMs;

        if (messages.size() > 0) {
          LOGGER.info("fetch {} messages in {} ms", messages.size(), System.currentTimeMillis() - start);
          processMessages(messages);
          _inputKafkaConsumer.ackOffset(messageAndOffset.getOffsetInfo());
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

  private MessageAndOffset<QueueConsumerRecord<Integer, KeyCoordinatorQueueMsg>> getMessagesFromQueue(
      BlockingQueue<QueueConsumerRecord<Integer, KeyCoordinatorQueueMsg>> queue, long deadline) {
    List<QueueConsumerRecord<Integer, KeyCoordinatorQueueMsg>> buffer = new ArrayList<>(_fetchMsgMaxCount * 2);
    while(System.currentTimeMillis() < deadline && buffer.size() < _fetchMsgMaxCount) {
      queue.drainTo(buffer, _fetchMsgMaxCount - buffer.size());
      if (buffer.size() < _fetchMsgMaxCount) {
        Uninterruptibles.sleepUninterruptibly(_fetchMsgDelayMs, TimeUnit.MILLISECONDS);
      }
    }
    OffsetInfo offsetInfo = new OffsetInfo();
    for (QueueConsumerRecord<Integer, KeyCoordinatorQueueMsg> record: buffer) {
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

  private void processMessages(List<QueueConsumerRecord<Integer, KeyCoordinatorQueueMsg>> messages) {
    Map<String, List<QueueConsumerRecord<Integer, KeyCoordinatorQueueMsg>>> tableMsgMap = new HashMap<>();
    for (QueueConsumerRecord<Integer, KeyCoordinatorQueueMsg> msg: messages) {
      tableMsgMap.computeIfAbsent(msg.getRecord().getPinotTableName(), t -> new ArrayList<>()).add(msg);
    }
    for (Map.Entry<String, List<QueueConsumerRecord<Integer, KeyCoordinatorQueueMsg>>> perTableUpdates: tableMsgMap.entrySet()) {
      processMessagesForTable(perTableUpdates.getKey(), perTableUpdates.getValue());
    }
  }

  private void processMessagesForTable(String tableName, List<QueueConsumerRecord<Integer, KeyCoordinatorQueueMsg>> msgList) {
    try {
      long start = System.currentTimeMillis();
      if (msgList == null || msgList.size() == 0) {
        LOGGER.warn("trying to process topic message with empty list {}", tableName);
        return;
      }
      KeyValueStoreTable<ByteArrayWrapper, KeyCoordinatorMessageContext> table = _keyValueStoreDB.getTable(tableName);

      List<ProduceTask<Integer, LogCoordinatorMessage>> tasks = new ArrayList<>();
      // get a set of unique primary key and retrieve its corresponding value in kv store
      Set<ByteArrayWrapper> primaryKeys = msgList.stream().map(msg -> new ByteArrayWrapper(msg.getRecord().getKey()))
          .collect(Collectors.toCollection(HashSet::new));
      Map<ByteArrayWrapper, KeyCoordinatorMessageContext> primaryKeyToValueMap = new HashMap<>(table.multiGet(new ArrayList<>(primaryKeys)));
      LOGGER.info("input keys got {} results from kv store in {} ms", primaryKeyToValueMap.size(), System.currentTimeMillis() - start);
      start = System.currentTimeMillis();

      for (QueueConsumerRecord<Integer, KeyCoordinatorQueueMsg> msg: msgList) {
        if (msg.getRecord().isVersionMessage()) {
          _currentVersionConsumed.set(msg.getPartition(), msg.getRecord().getVersion());
          continue;
        }
        KeyCoordinatorMessageContext currentContext = msg.getRecord().getContext();
        ByteArrayWrapper key = new ByteArrayWrapper(msg.getRecord().getKey());
        if (primaryKeyToValueMap.containsKey(key)) {
          // key conflicts, should resolve which one to delete
          KeyCoordinatorMessageContext oldContext = primaryKeyToValueMap.get(key);
          if (!oldContext.equals(currentContext)) {
            // message are equals, it is from another replica and should skip
            if (_messageResolveStrategy.shouldDeleteFirstMessage(oldContext, currentContext)) {
              // the existing message we have is older than the message we just processed, create delete for it
              tasks.add(createMessageToLogCoordinator(tableName, oldContext.getSegmentName(), oldContext.getKafkaOffset(),
                  _currentVersionConsumed.get(msg.getPartition()), LogEventType.DELETE, msg.getPartition()));
              // update the local cache to the latest message, so message within the same batch can override each other
              primaryKeyToValueMap.put(key, currentContext);
            } else {
              // the new message is older than the existing message
              tasks.add(createMessageToLogCoordinator(tableName, currentContext.getSegmentName(), currentContext.getKafkaOffset(),
                  _currentVersionConsumed.get(msg.getPartition()), LogEventType.DELETE, msg.getPartition()));
            }
          }
        } else {
          // no key in the existing map, adding this key to the running set
          primaryKeyToValueMap.put(key, currentContext);
        }
        // always create a insert event for validFrom if is not a duplicated input
        tasks.add(createMessageToLogCoordinator(tableName, currentContext.getSegmentName(), currentContext.getKafkaOffset(),
            _currentVersionConsumed.get(msg.getPartition()), LogEventType.INSERT, msg.getPartition()));
      }
      LOGGER.info("processed all messages in {} ms", System.currentTimeMillis() - start);
      List<ProduceTask<Integer, LogCoordinatorMessage>> failedTasks = sendMessagesToLogCoordinator(tasks, 10, TimeUnit.SECONDS);
      if (failedTasks.size() > 0) {
        LOGGER.error("send to producer failed: {}", failedTasks.size());
      }
      storeMessageToLocal(tableName, tasks);
      storeMessageToKVStore(table, primaryKeyToValueMap);
    } catch (IOException e) {
      throw new RuntimeException("failed to interact with rocksdb", e);
    } catch (RuntimeException e) {
      throw new RuntimeException("failed to interact with key value store", e);
    }
  }

  // partition is the partition number of the ingestion upsert event. Note that the partition of records with primary
  // key will be the same across ingestion upsert event and segment update event topics.
  private ProduceTask<Integer, LogCoordinatorMessage> createMessageToLogCoordinator(String tableName, String segmentName,
                                                                                    long oldKafkaOffset, long value,
                                                                                    LogEventType eventType, int partition) {
    return new ProduceTask<>(DistributedCommonUtils.getKafkaTopicFromTableName(tableName),
            partition, new LogCoordinatorMessage(segmentName, oldKafkaOffset, value, eventType));
  }

  private List<ProduceTask<Integer, LogCoordinatorMessage>> sendMessagesToLogCoordinator(
      List<ProduceTask<Integer, LogCoordinatorMessage>> tasks, long timeout, TimeUnit timeUnit) {
    long startTime = System.currentTimeMillis();
    // send all and wait for result, batch for better perf
    CountDownLatch countDownLatch = new CountDownLatch(tasks.size());
    tasks.forEach(t -> t.setCountDownLatch(countDownLatch));
    _outputKafkaProducer.batchProduce(tasks);
    _outputKafkaProducer.flush();
    try {
      boolean allFinished = countDownLatch.await(timeout, timeUnit);
      if (allFinished) {
        return new ArrayList<>();
      } else {
        return tasks.stream().filter(t -> !t.isSucceed()).collect(Collectors.toList());
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("encountered run time exception while waiting for the loop to finish");
    } finally {
      LOGGER.info("send to producer take {} ms", System.currentTimeMillis() - startTime);
    }
  }

  protected KeyValueStoreDB<ByteArrayWrapper, KeyCoordinatorMessageContext> getKeyValueStore(Configuration conf) {
    RocksDBKeyValueStoreDB keyValueStore = new RocksDBKeyValueStoreDB();
    keyValueStore.init(conf);
    return keyValueStore;
  }

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
    LOGGER.info("stored all data to files in {} ms", System.currentTimeMillis() - start);
  }

  private void storeMessageToKVStore(KeyValueStoreTable<ByteArrayWrapper, KeyCoordinatorMessageContext> table,
                                     Map<ByteArrayWrapper, KeyCoordinatorMessageContext> primaryKeyToValueMap)
      throws IOException {
    long start = System.currentTimeMillis();
      // update kv store
    table.multiPut(primaryKeyToValueMap);
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
}

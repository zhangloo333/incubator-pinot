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
import org.apache.pinot.grigio.common.OffsetInfo;
import org.apache.pinot.grigio.common.messages.KeyCoordinatorQueueMsg;
import org.apache.pinot.grigio.common.metrics.GrigioGauge;
import org.apache.pinot.grigio.common.rpcQueue.KeyCoordinatorQueueConsumer;
import org.apache.pinot.grigio.common.rpcQueue.QueueConsumerRecord;
import org.apache.pinot.grigio.keyCoordinator.GrigioKeyCoordinatorMetrics;
import org.apache.pinot.grigio.keyCoordinator.helix.State;
import org.apache.pinot.grigio.keyCoordinator.starter.KeyCoordinatorConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * class to handle fetching messages from the given input message queue and allow other components to get list of
 * messages from its internal buffer
 */
public class MessageFetcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(MessageFetcher.class);
  private static final long TERMINATION_WAIT_MS = 10000;

  protected int _fetchMsgDelayMs;
  protected int _fetchMsgMaxDelayMs;
  protected int _fetchMsgMaxCount;

  protected BlockingQueue<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> _consumerRecordBlockingQueue;
  protected KeyCoordinatorQueueConsumer _inputKafkaConsumer;
  protected GrigioKeyCoordinatorMetrics _metrics;
  protected ExecutorService _consumerThread;

  protected volatile State _state;

  public MessageFetcher(KeyCoordinatorConf conf, KeyCoordinatorQueueConsumer consumer,
                        GrigioKeyCoordinatorMetrics metrics) {
    this(conf, consumer, Executors.newSingleThreadExecutor(), metrics);
  }

  @VisibleForTesting
  protected MessageFetcher(KeyCoordinatorConf conf, KeyCoordinatorQueueConsumer consumer,
                        ExecutorService service, GrigioKeyCoordinatorMetrics metrics) {
    _inputKafkaConsumer = consumer;
    _metrics = metrics;
    _consumerThread = service;
    _consumerRecordBlockingQueue = new ArrayBlockingQueue<>(conf.getConsumerBlockingQueueSize());

    _fetchMsgDelayMs = conf.getInt(KeyCoordinatorConf.FETCH_MSG_DELAY_MS,
        KeyCoordinatorConf.FETCH_MSG_DELAY_MS_DEFAULT);
    _fetchMsgMaxDelayMs = conf.getInt(KeyCoordinatorConf.FETCH_MSG_MAX_DELAY_MS,
        KeyCoordinatorConf.FETCH_MSG_MAX_DELAY_MS_DEFAULT);
    _fetchMsgMaxCount = conf.getInt(KeyCoordinatorConf.FETCH_MSG_MAX_BATCH_SIZE,
        KeyCoordinatorConf.FETCH_MSG_MAX_BATCH_SIZE_DEFAULT);

    _state = State.INIT;
    LOGGER.info("starting with fetch delay: {} max delay: {}, fetch max count: {}", _fetchMsgDelayMs, _fetchMsgMaxDelayMs,
        _fetchMsgMaxCount);
  }

  public void start() {
    Preconditions.checkState(_state == State.INIT, "key coordinate is not in correct state");
    _state = State.RUNNING;
    _consumerThread.submit(this::consumerIngestLoop);

  }

  /**
   * get a list of messages read by the consumer ingestion thread
   * @param deadlineInMs linux epoch time we should stop the ingestion and return it to caller with the data we have so far
   * @return list of messages to be processed
  */
  public MessageAndOffset<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> getMessages(long deadlineInMs) {
    List<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> buffer = new ArrayList<>(_fetchMsgMaxCount);
    while(System.currentTimeMillis() < deadlineInMs && buffer.size() < _fetchMsgMaxCount) {
      _consumerRecordBlockingQueue.drainTo(buffer, _fetchMsgMaxCount - buffer.size());
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

  /**
   * commit the current ingestion progress to internal offset storage
   * @param messageAndOffset
   */
  public void ackOffset(MessageAndOffset messageAndOffset) {
    _inputKafkaConsumer.ackOffset(messageAndOffset.getOffsetInfo());
  }

  public void stop() {
    _state = State.SHUTTING_DOWN;
    _consumerThread.shutdown();
    try {
      _consumerThread.awaitTermination(TERMINATION_WAIT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      LOGGER.error("failed to wait for key coordinator thread to shutdown", ex);
    }
    _consumerThread.shutdownNow();
  }

  private void consumerIngestLoop() {
    while (_state == State.RUNNING) {
      try {
        _metrics.setValueOfGlobalGauge(GrigioGauge.MESSAGE_PROCESS_QUEUE_SIZE, _consumerRecordBlockingQueue.size());
        List<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> records =
            _inputKafkaConsumer.getRequests(_fetchMsgMaxDelayMs, TimeUnit.MILLISECONDS);
        if (records.size() == 0) {
          LOGGER.info("no message found in kafka consumer, sleep and wait for next batch");
          Uninterruptibles.sleepUninterruptibly(_fetchMsgDelayMs, TimeUnit.MILLISECONDS);
        } else {
          records.forEach(c -> {
            try {
              _consumerRecordBlockingQueue.put(c);
            } catch (InterruptedException e) {
              LOGGER.warn("exception while trying to put message to queue", e);
            }
          });
          _metrics.setValueOfGlobalGauge(GrigioGauge.KC_INPUT_MESSAGE_LAG_MS,
              System.currentTimeMillis() - records.get(records.size() - 1).getTimestamp());
        }
      } catch (Exception ex) {
        LOGGER.error("encountered exception in consumer ingest loop, will retry", ex);
      }
    }
    LOGGER.info("exiting consumer ingest loop");
  }

  /**
   * class wrap around the message and offset associated information
   * @param <K>
   */
  public static class MessageAndOffset<K> {
    private List<K> _messages;
    private OffsetInfo _offsetInfo;

    /**
     * @param messages list of messages for the current batch
     * @param offsetInfo the largest offset for each partition in current set of messages
     */
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

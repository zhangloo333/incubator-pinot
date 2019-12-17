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
import org.apache.pinot.grigio.common.messages.KeyCoordinatorQueueMsg;
import org.apache.pinot.grigio.common.metrics.GrigioMeter;
import org.apache.pinot.grigio.common.metrics.GrigioTimer;
import org.apache.pinot.grigio.common.rpcQueue.QueueConsumerRecord;
import org.apache.pinot.grigio.common.utils.CommonUtils;
import org.apache.pinot.grigio.keyCoordinator.GrigioKeyCoordinatorMetrics;
import org.apache.pinot.grigio.keyCoordinator.helix.State;
import org.apache.pinot.grigio.keyCoordinator.starter.KeyCoordinatorConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class DistributedKeyCoordinatorCore {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedKeyCoordinatorCore.class);
  private static final long TERMINATION_WAIT_MS = 10000;

  protected KeyCoordinatorConf _conf;
  protected ExecutorService _messageProcessThread;
  protected GrigioKeyCoordinatorMetrics _metrics;
  protected int _fetchMsgMaxDelayMs;

  // sub manager for other stuff
  protected SegmentEventProcessor _segmentEventProcessor;
  protected MessageFetcher _messageFetcher;
  protected VersionMessageManager _versionMessageManager;

  protected volatile State _state = State.SHUTDOWN;

  public DistributedKeyCoordinatorCore() {}

  public void init(KeyCoordinatorConf conf, SegmentEventProcessor segmentEventProcessor,
                   MessageFetcher fetcher, VersionMessageManager versionMessageManager,
                   GrigioKeyCoordinatorMetrics metrics) {
    init(conf, Executors.newSingleThreadExecutor(), segmentEventProcessor, fetcher, versionMessageManager, metrics);
  }

  @VisibleForTesting
  public void init(KeyCoordinatorConf conf, ExecutorService coreThread, SegmentEventProcessor segmentEventProcessor,
                   MessageFetcher fetcher, VersionMessageManager versionMessageManager,
                   GrigioKeyCoordinatorMetrics metrics) {
    CommonUtils.printConfiguration(conf, "distributed key coordinator core");
    Preconditions.checkState(_state == State.SHUTDOWN, "can only init if it is not running yet");
    _conf = conf;
    _messageProcessThread = coreThread;
    _versionMessageManager = versionMessageManager;
    _messageFetcher = fetcher;
    _metrics = metrics;

    _fetchMsgMaxDelayMs = conf.getInt(KeyCoordinatorConf.FETCH_MSG_MAX_DELAY_MS,
        KeyCoordinatorConf.FETCH_MSG_MAX_DELAY_MS_DEFAULT);

    _state = State.INIT;
  }

  public void start() {
    Preconditions.checkState(_state == State.INIT, "key coordinate is not in correct state");
    LOGGER.info("starting key coordinator message process loop");
    _state = State.RUNNING;
    _messageFetcher.start();
    _versionMessageManager.start();
    _segmentEventProcessor.start();
    _messageProcessThread.submit(this::messageProcessLoop);
  }

  private void messageProcessLoop() {
    try {
      long deadline = System.currentTimeMillis() + _fetchMsgMaxDelayMs;
      while (_state == State.RUNNING) {
        LOGGER.info("starting new loop");
        long start = System.currentTimeMillis();
        // process message when we got max message count or reach max delay ms
        MessageFetcher.MessageAndOffset<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> messageAndOffset =
            _messageFetcher.getMessages(deadline);
        List<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> messages = messageAndOffset.getMessages();
        deadline = System.currentTimeMillis() + _fetchMsgMaxDelayMs;

        _metrics.addMeteredGlobalValue(GrigioMeter.MESSAGE_PROCESS_THREAD_FETCH_COUNT, messages.size());
        _metrics.addTimedValueMs(GrigioTimer.MESSAGE_PROCESS_THREAD_FETCH_DELAY, System.currentTimeMillis() - start);

        if (messages.size() > 0) {
          _segmentEventProcessor.processMessages(messages);
          // todo: make ackOffset and setVersionConsumed as one transaction
          _messageFetcher.ackOffset(messageAndOffset);
          _versionMessageManager.setVersionConsumedToPropertyStore();
          LOGGER.info("kc processed {} messages in this loop for {} ms", messages.size(),
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

  public void stop() {
    _state = State.SHUTTING_DOWN;
    _messageFetcher.stop();
    _versionMessageManager.stop();
    _segmentEventProcessor.stop();
    _messageProcessThread.shutdown();
    try {
      _messageProcessThread.awaitTermination(TERMINATION_WAIT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      LOGGER.error("failed to wait for key coordinator thread to shutdown", ex);
    }
    _messageProcessThread.shutdownNow();
    _state = State.SHUTDOWN;
  }

  public State getState() {
    return _state;
  }


}

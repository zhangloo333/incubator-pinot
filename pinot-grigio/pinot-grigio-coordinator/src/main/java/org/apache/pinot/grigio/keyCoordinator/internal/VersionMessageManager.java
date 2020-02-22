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
import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixManager;
import org.apache.pinot.grigio.common.messages.KeyCoordinatorQueueMsg;
import org.apache.pinot.grigio.common.metrics.GrigioGauge;
import org.apache.pinot.grigio.common.metrics.GrigioTimer;
import org.apache.pinot.grigio.common.rpcQueue.ProduceTask;
import org.apache.pinot.grigio.common.rpcQueue.VersionMsgQueueProducer;
import org.apache.pinot.grigio.keyCoordinator.GrigioKeyCoordinatorMetrics;
import org.apache.pinot.grigio.keyCoordinator.helix.KeyCoordinatorLeadershipManager;
import org.apache.pinot.grigio.keyCoordinator.helix.KeyCoordinatorVersionManager;
import org.apache.pinot.grigio.keyCoordinator.helix.State;
import org.apache.pinot.grigio.keyCoordinator.starter.KeyCoordinatorConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * class to handle version information in key coordinator, it does the following two tasks:
 * 1. send out version message to kc input queue if it is leader at the moment
 * 2. store and manage the current version for each input partition.
 */
public class VersionMessageManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(VersionMessageManager.class);

  protected KeyCoordinatorConf _conf;
  protected Timer _versionMessageTimer;
  protected TimerTask _versionMessageProduceTask;
  protected VersionMsgQueueProducer _versionMessageKafkaProducer;
  protected KeyCoordinatorVersionManager _keyCoordinatorVersionManager;
  protected KeyCoordinatorLeadershipManager _keyCoordinatorLeadershipManager;
  protected GrigioKeyCoordinatorMetrics _metrics;
  protected long _versionMessageIntervalMs;
  protected Map<Integer, Long> _currentVersionConsumed;

  protected volatile State _state;

  public VersionMessageManager(KeyCoordinatorConf conf, VersionMsgQueueProducer versionMsgQueueProducer,
                               HelixManager controllerHelixManager, GrigioKeyCoordinatorMetrics metrics) {
    this(conf,
        versionMsgQueueProducer,
        new Timer(),
        new KeyCoordinatorVersionManager(controllerHelixManager),
        new KeyCoordinatorLeadershipManager(controllerHelixManager),
        metrics);
  }

  @VisibleForTesting
  public VersionMessageManager(KeyCoordinatorConf conf, VersionMsgQueueProducer versionProducer,
                               Timer versionMessageTimer, KeyCoordinatorVersionManager versionManager,
                               KeyCoordinatorLeadershipManager leadershipManager, GrigioKeyCoordinatorMetrics metrics) {
    _conf = conf;
    _versionMessageTimer = versionMessageTimer;
    _versionMessageKafkaProducer = versionProducer;
    _keyCoordinatorVersionManager = versionManager;
    _keyCoordinatorLeadershipManager = leadershipManager;
    _metrics = metrics;
    _versionMessageProduceTask = new TimerTask() {
      @Override
      public void run() {
        produceVersionMessage();
      }
    };
    _versionMessageIntervalMs = conf.getLong(KeyCoordinatorConf.VERSION_MESSAGE_INTERVAL_MS,
        KeyCoordinatorConf.VERSION_MESSAGE_INTERVAL_MS_DEFAULT);
    _currentVersionConsumed = _keyCoordinatorVersionManager.getVersionConsumedFromPropertyStore();

    _state = State.INIT;
  }

  public void start() {
    _state = State.RUNNING;
    _versionMessageTimer.schedule(_versionMessageProduceTask, 0L, _versionMessageIntervalMs);
  }

  public void stop() {
    _versionMessageTimer.cancel();
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

  /**
   * store the current version for all known partition to property store
   */
  public synchronized void setVersionConsumedToPropertyStore() {
    _keyCoordinatorVersionManager.setVersionConsumedToPropertyStore(ImmutableMap.copyOf(_currentVersionConsumed));
  }

  /**
   * update the version for a given partition if it is larger than current version value
   * @param partition the partition we are updating
   * @param version the version we are updating to
   */
  public synchronized void maybeUpdateVersionConsumed(int partition, long version) {
    if (!_currentVersionConsumed.containsKey(partition) || _currentVersionConsumed.get(partition) < version) {
      _currentVersionConsumed.put(partition, version);
    }
  }

  /**
   * get the current version for a given partition
   * @param partition the partition we are trying to fetch the version from
   * @return the version associated with the current partition
   */
  public synchronized long getVersionConsumed(int partition) {
    return _currentVersionConsumed.getOrDefault(partition, 0L);
  }
}

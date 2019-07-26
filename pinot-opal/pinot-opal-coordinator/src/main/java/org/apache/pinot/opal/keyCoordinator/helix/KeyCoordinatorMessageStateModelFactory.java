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
package org.apache.pinot.opal.keyCoordinator.helix;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.pinot.opal.common.rpcQueue.KeyCoordinatorQueueConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * State model for key coordinator to handle:
 * 1. start of the key coordinator cluster (initial assignment of key coordinator message segments)
 * 2. todo: fail over of a key coordinator instance
 */

public class KeyCoordinatorMessageStateModelFactory extends StateModelFactory<StateModel> {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorMessageStateModelFactory.class);

  private final KeyCoordinatorQueueConsumer _keyCoordinatorQueueConsumer;
  private final String _keyCoordinatorMessageTopic;

  public KeyCoordinatorMessageStateModelFactory(KeyCoordinatorQueueConsumer keyCoordinatorQueueConsumer,
      String keyCoordinatorMessageTopic) {
    _keyCoordinatorQueueConsumer = keyCoordinatorQueueConsumer;
    _keyCoordinatorMessageTopic = keyCoordinatorMessageTopic;
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    return new KeyCoordinatorMessageStateModel(partitionName);
  }

  @StateModelInfo(states = "{'OFFLINE', 'ONLINE'}", initialState = "OFFLINE")
  public class KeyCoordinatorMessageStateModel extends StateModel {

    private final String _partitionName;

    public KeyCoordinatorMessageStateModel(String partitionName) {
      LOGGER.info("Creating a Key coordinator message state model with partition: {}", partitionName);
      _partitionName = partitionName;
    }

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      LOGGER.info("Key coordinator message onBecomeOnlineFromOffline with partition: {}", _partitionName);
      try {
        Integer partition = Integer.valueOf(_partitionName);
        _keyCoordinatorQueueConsumer.subscribe(_keyCoordinatorMessageTopic, partition);
      } catch (final NumberFormatException e) {
        LOGGER.error("Failed to parse the partition name", e);
      }
    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      LOGGER.info("Key coordinator message onBecomeOfflineFromOnline with partition: {}", _partitionName);
      try {
        Integer partition = Integer.valueOf(_partitionName);
        _keyCoordinatorQueueConsumer.unsubscribe(_keyCoordinatorMessageTopic, partition);
      } catch (final NumberFormatException e) {
        LOGGER.error("Failed to parse the partition name", e);
      }
    }
  }
}

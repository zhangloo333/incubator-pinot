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
package org.apache.pinot.grigio.keyCoordinator.helix;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

public class KeyCoordinatorPinotHelixSpectator {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorPinotHelixSpectator.class);

  private final String _pinotHelixZkURL;
  private final String _pinotHelixClusterName;
  private final String _keyCoordinatorId;
  private final HelixManager _spectatorHelixManager;

  private HelixManager _helixZkManager;

  public KeyCoordinatorPinotHelixSpectator(@Nonnull String zkURL, @Nonnull String helixClusterName,
                                           @Nonnull String keyCoordinatorId) throws Exception {
    _pinotHelixZkURL = zkURL;
    _pinotHelixClusterName = helixClusterName;
    _keyCoordinatorId = keyCoordinatorId;

    _spectatorHelixManager =
        HelixManagerFactory.getZKHelixManager(_pinotHelixClusterName, _keyCoordinatorId, InstanceType.SPECTATOR, _pinotHelixZkURL);
    _spectatorHelixManager.connect();
  }

  public HelixManager getHelixManager() {
    return _spectatorHelixManager;
  }

  public void addListener(IdealStateChangeListener listener) throws Exception {
    _spectatorHelixManager.addIdealStateChangeListener(listener);
  }

}

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
package org.apache.pinot.opal.distributed.keyCoordinator.helix;

import java.util.List;
import javax.annotation.Nonnull;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.pinot.opal.common.utils.HelixSetupUtils;
import org.apache.pinot.opal.distributed.keyCoordinator.api.KeyCoordinatorInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This manages the key coordinator cluster (key coordinators as controller-participant)
 */
public class KeyCoordinatorClusterHelixManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorClusterHelixManager.class);

  private final String _helixZkURL;
  private final String _keyCoordinatorClusterName;
  private final String _keyCoordinatorId;
  private final HelixManager _controllerParticipantHelixManager;
  private final HelixAdmin _helixAdmin;

  public KeyCoordinatorClusterHelixManager(@Nonnull String zkURL, @Nonnull String keyCoordinatorClusterName, @Nonnull String keyCoordinatorId) {
    _helixZkURL = zkURL;
    _keyCoordinatorClusterName = keyCoordinatorClusterName;
    _keyCoordinatorId = keyCoordinatorId;

    _controllerParticipantHelixManager = HelixSetupUtils.setup(_keyCoordinatorClusterName, _helixZkURL, _keyCoordinatorId);
    _helixAdmin = _controllerParticipantHelixManager.getClusterManagmentTool();
  }

  public List<String> getAllInstances() {
    return _helixAdmin.getInstancesInCluster(_keyCoordinatorClusterName);
  }

  public void addInstance(KeyCoordinatorInstance keyCoordinatorInstance) {
    _helixAdmin.addInstance(_keyCoordinatorClusterName, keyCoordinatorInstance.toInstanceConfig());
  }

  public void dropInstance(KeyCoordinatorInstance keyCoordinatorInstance) {
    _helixAdmin.dropInstance(_keyCoordinatorClusterName, keyCoordinatorInstance.toInstanceConfig());
  }
}

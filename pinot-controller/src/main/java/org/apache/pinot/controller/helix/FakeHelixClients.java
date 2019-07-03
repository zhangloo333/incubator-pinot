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
package org.apache.pinot.controller.helix;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.config.TagNameUtils;
import org.apache.pinot.common.utils.CommonConstants;


public class FakeHelixClients {
  private static final int BASE_SERVER_ADMIN_PORT = 10000;
  private static final int NUM_INSTANCES = 5;
  private static final String BROKER_TENANT_NAME = "brokerTenant";
  private static final String SERVER_TENANT_NAME = "serverTenant";
  private static final String TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME);
  private static final int CONNECTION_TIMEOUT_IN_MILLISECOND = 10_000;
  private static final int MAX_TIMEOUT_IN_MILLISECOND = 5_000;
  private static final int MAXIMUM_NUMBER_OF_CONTROLLER_INSTANCES = 10;
  private static final long TIMEOUT_IN_MS = 10_000L;

  private List<HelixManager> _helixManagers = new ArrayList<>();

  public FakeHelixClients() {
  }

  public void shutDown() {
    for (HelixManager helixManager : _helixManagers) {
      helixManager.disconnect();
    }
  }

  public void addFakeBrokerInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances)
      throws Exception {
    addFakeBrokerInstancesToAutoJoinHelixCluster(helixClusterName, zkServer, numInstances, false);
  }

  public void addFakeBrokerInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances, boolean isSingleTenant)
      throws Exception {
    for (int i = 0; i < numInstances; ++i) {
      final String brokerId = "Broker_localhost_" + i;
      final HelixManager helixZkManager =
          HelixManagerFactory.getZKHelixManager(helixClusterName, brokerId, InstanceType.PARTICIPANT, zkServer);
      _helixManagers.add(helixZkManager);
      final StateMachineEngine stateMachineEngine = helixZkManager.getStateMachineEngine();
      final StateModelFactory<?> stateModelFactory = new EmptyBrokerOnlineOfflineStateModelFactory();
      stateMachineEngine
          .registerStateModelFactory(EmptyBrokerOnlineOfflineStateModelFactory.getStateModelDef(), stateModelFactory);
      helixZkManager.connect();
      if (isSingleTenant) {
        helixZkManager.getClusterManagmentTool().addInstanceTag(helixClusterName, brokerId,
            TagNameUtils.getBrokerTagForTenant(TagNameUtils.DEFAULT_TENANT_NAME));
      } else {
        helixZkManager.getClusterManagmentTool().addInstanceTag(helixClusterName, brokerId, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
      }
    }
  }

  public void addFakeDataInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances)
      throws Exception {
    addFakeDataInstancesToAutoJoinHelixCluster(helixClusterName, zkServer, numInstances, false,
        CommonConstants.Server.DEFAULT_ADMIN_API_PORT);
  }

  public void addFakeDataInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances, boolean isSingleTenant)
      throws Exception {
    addFakeDataInstancesToAutoJoinHelixCluster(helixClusterName, zkServer, numInstances, isSingleTenant,
        CommonConstants.Server.DEFAULT_ADMIN_API_PORT);
  }

  public void addFakeDataInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances, boolean isSingleTenant, int adminPort)
      throws Exception {

    for (int i = 0; i < numInstances; ++i) {
      final String instanceId = "Server_localhost_" + i;
      addFakeDataInstanceToAutoJoinHelixCluster(helixClusterName, zkServer, instanceId, isSingleTenant, adminPort + i);
    }
  }

  public void addFakeDataInstanceToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      String instanceId)
      throws Exception {
    addFakeDataInstanceToAutoJoinHelixCluster(helixClusterName, zkServer, instanceId, false,
        CommonConstants.Server.DEFAULT_ADMIN_API_PORT);
  }

  public void addFakeDataInstanceToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      String instanceId, boolean isSingleTenant)
      throws Exception {
    addFakeDataInstanceToAutoJoinHelixCluster(helixClusterName, zkServer, instanceId, isSingleTenant,
        CommonConstants.Server.DEFAULT_ADMIN_API_PORT);
  }

  public void addFakeDataInstanceToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      String instanceId, boolean isSingleTenant, int adminPort)
      throws Exception {
    final HelixManager helixZkManager =
        HelixManagerFactory.getZKHelixManager(helixClusterName, instanceId, InstanceType.PARTICIPANT, zkServer);
    _helixManagers.add(helixZkManager);
    final StateMachineEngine stateMachineEngine = helixZkManager.getStateMachineEngine();
    final StateModelFactory<?> stateModelFactory = new EmptySegmentOnlineOfflineStateModelFactory();
    stateMachineEngine
        .registerStateModelFactory(EmptySegmentOnlineOfflineStateModelFactory.getStateModelDef(), stateModelFactory);
    helixZkManager.connect();
    if (isSingleTenant) {
      helixZkManager.getClusterManagmentTool().addInstanceTag(helixClusterName, instanceId,
          TableNameBuilder.OFFLINE.tableNameWithType(TagNameUtils.DEFAULT_TENANT_NAME));
      helixZkManager.getClusterManagmentTool().addInstanceTag(helixClusterName, instanceId,
          TableNameBuilder.REALTIME.tableNameWithType(TagNameUtils.DEFAULT_TENANT_NAME));
    } else {
      helixZkManager.getClusterManagmentTool().addInstanceTag(helixClusterName, instanceId, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
    }
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT, helixClusterName)
            .forParticipant(instanceId).build();
    Map<String, String> props = new HashMap<>();
    props.put(CommonConstants.Helix.Instance.ADMIN_PORT_KEY, String.valueOf(adminPort));
    helixZkManager.getClusterManagmentTool().setConfig(scope, props);
  }

}

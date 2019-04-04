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
package com.linkedin.pinot.broker.upsert;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.broker.broker.helix.HelixBrokerStarter;
import org.apache.pinot.common.restlet.resources.ResourceUtils;
import com.linkedin.pinot.common.restlet.resources.TableLowWaterMarksInfo;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.controller.helix.ControllerRequestBuilderUtil;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixAdmin;
import org.testng.Assert;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

public class PollingBasedLowWaterMarkServiceTest {
  private PinotHelixResourceManager _pinotResourceManager;
  private static final String HELIX_CLUSTER_NAME = "TestLowWaterMarksPolling";
  private final Configuration _pinotHelixBrokerProperties = new PropertiesConfiguration();

  private HelixAdmin _helixAdmin;
  private HelixBrokerStarter _helixBrokerStarter;

//  @Test
  public void testBrokerCallServersCorrectly()
      throws Exception {
    ZkStarter.startLocalZkServer();
    final String instanceId = "localhost_helixController";
    _pinotResourceManager =
        new PinotHelixResourceManager(ZkStarter.DEFAULT_ZK_STR, HELIX_CLUSTER_NAME, instanceId, null, 10000L,
            true, /*isUpdateStateModel=*/false, true);
    _pinotResourceManager.start();
    _helixAdmin = _pinotResourceManager.getHelixAdmin();

    // Set up a cluster with one controller and 2 servers.
    ControllerRequestBuilderUtil
        .addFakeBrokerInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZkStarter.DEFAULT_ZK_STR, 1, true);

    for (int i = 0; i < 2; ++i) {
      final String serverInstanceId = "Server_localhost_" + (i+1);
      ControllerRequestBuilderUtil.addFakeDataInstanceToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZkStarter.DEFAULT_ZK_STR,
          serverInstanceId, true, CommonConstants.Server.DEFAULT_ADMIN_API_PORT + i);
    }


    _pinotHelixBrokerProperties.addProperty(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT, 8943);
    _pinotHelixBrokerProperties
        .addProperty(CommonConstants.Broker.CONFIG_OF_BROKER_REFRESH_TIMEBOUNDARY_INFO_SLEEP_INTERVAL, 100L);


    // Set the two servers' lwms info.
    Map<Integer, Long> table1Map = new HashMap<>();
    table1Map.put(0, 10L);
    table1Map.put(1, 20L);
    Map<Integer, Long> table2Map = new HashMap<>();
    table2Map.put(0, 11L);
    Map<String, Map<Integer, Long>> server1LwmsMap = new ConcurrentHashMap<>();
    server1LwmsMap.put("Table1", table1Map);
    server1LwmsMap.put("Table2", table2Map);

    Map<Integer, Long> newTable1Map = new HashMap<>();
    newTable1Map.put(0, 15L);
    newTable1Map.put(1, 18L);
    Map<Integer, Long> table3Map = new HashMap<>();
    table3Map.put(0, 17L);
    Map<String, Map<Integer, Long>> server2LwmsMap = new HashMap<>();
    server2LwmsMap.put("Table1", newTable1Map);
    server2LwmsMap.put("Table3", table3Map);


    WireMockServer mockServer1 = new WireMockServer(1);
    mockServer1.start();
    mockServer1.stubFor(get(urlEqualTo("/lwms")).willReturn(aResponse()
        .withBody(ResourceUtils.convertToJsonString(new TableLowWaterMarksInfo(server1LwmsMap)))
        .withHeader("Content-Type", "application/json")
        .withStatus(200)));
    WireMockServer mockServer2 = new WireMockServer(2);
    mockServer2.start();
    mockServer2.stubFor(get(urlEqualTo("/lwms")).willReturn(aResponse()
        .withBody(ResourceUtils.convertToJsonString(new TableLowWaterMarksInfo(server2LwmsMap)))
        .withHeader("Content-Type", "application/json")
        .withStatus(200)));

    _helixBrokerStarter =
        new HelixBrokerStarter(_pinotHelixBrokerProperties, HELIX_CLUSTER_NAME, ZkStarter.DEFAULT_ZK_STR);


    while (_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_OFFLINE").size() == 0
        || _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_BROKER").size() == 0) {
      Thread.sleep(100);
    }

    Thread.sleep(1000);

    // Verify the low water mark service behaviors.
    mockServer1.verify(1, getRequestedFor(urlEqualTo("/lwms")));
    mockServer2.verify(1, getRequestedFor(urlEqualTo("/lwms")));

    Assert.assertNotNull(_helixBrokerStarter.getLwmService().getLowWaterMarks("Table1"));
    Assert.assertNotNull(_helixBrokerStarter.getLwmService().getLowWaterMarks("Table2"));
    Assert.assertNotNull(_helixBrokerStarter.getLwmService().getLowWaterMarks("Table3"));

    // Table 1 verification.
    Assert.assertEquals(_helixBrokerStarter.getLwmService().getLowWaterMarks("Table1").size(), 2);
    Assert.assertTrue(_helixBrokerStarter.getLwmService().getLowWaterMarks("Table1").get(Integer.parseInt("0")) == 10L);
    Assert.assertTrue(_helixBrokerStarter.getLwmService().getLowWaterMarks("Table1").get(Integer.parseInt("1")) == 18L);
    // Table 1 verification.
    Assert.assertEquals(_helixBrokerStarter.getLwmService().getLowWaterMarks("Table2").size(), 1);
    Assert.assertTrue(_helixBrokerStarter.getLwmService().getLowWaterMarks("Table2").get(Integer.parseInt("0")) == 11L);
    // Table 1 verification.
    Assert.assertEquals(_helixBrokerStarter.getLwmService().getLowWaterMarks("Table3").size(), 1);
    Assert.assertTrue(_helixBrokerStarter.getLwmService().getLowWaterMarks("Table3").get(Integer.parseInt("0")) == 17L);
  }

//  @Test
  public void testLowWaterMarksMerge() {
    Map<Integer, Long> table1Map = new HashMap<>();
    table1Map.put(0, 10L);
    table1Map.put(1, 20L);
    Map<Integer, Long> table2Map = new HashMap<>();
    table2Map.put(0, 11L);
    Map<String, Map<Integer, Long>> currentLwmsMap = new ConcurrentHashMap<>();
    currentLwmsMap.put("Table1", table1Map);
    currentLwmsMap.put("Table2", table2Map);

    Map<Integer, Long> newTable1Map = new HashMap<>();
    newTable1Map.put(0, 15L);
    newTable1Map.put(1, 18L);
    Map<Integer, Long> table3Map = new HashMap<>();
    table3Map.put(0, 17L);
    Map<String, Map<Integer, Long>> serverLwms = new HashMap<>();
    serverLwms.put("Table1", newTable1Map);
    serverLwms.put("Table3", table3Map);

    PollingBasedLowWaterMarkService.LwmMerger.updateLowWaterMarks(currentLwmsMap, serverLwms);

    Assert.assertEquals(currentLwmsMap.size(), 3);

    // Verify Table1 content.
    Assert.assertTrue(currentLwmsMap.containsKey("Table1"));
    Map<Integer, Long> lwmsMap1 = currentLwmsMap.get("Table1");
    Assert.assertEquals(lwmsMap1.size(), 2);
    // Verify that the lower LWM value is chosen in the combined results.
    Assert.assertTrue(lwmsMap1.get(0) == 10L);
    Assert.assertTrue(lwmsMap1.get(1) == 18L);

    // Verify Table2 content.
    Assert.assertTrue(currentLwmsMap.containsKey("Table2"));
    Map<Integer, Long> lwmsMap2 = currentLwmsMap.get("Table2");
    Assert.assertEquals(lwmsMap2.size(), 1);
    // Verify that the lower LWM value is chosen in the combined results.
    Assert.assertTrue(lwmsMap2.get(0) == 11L);

    // Verify Table3 content.
    Assert.assertTrue(currentLwmsMap.containsKey("Table3"));
    Map<Integer, Long> lwmsMap3 = currentLwmsMap.get("Table3");
    Assert.assertEquals(lwmsMap3.size(), 1);
    // Verify that the lower LWM value is chosen in the combined results.
    Assert.assertTrue(lwmsMap3.get(0) == 17L);
  }
}

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
package org.apache.pinot.grigio.common.storageProvider.retentionManager;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.IdealState;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogStorageProvider;
import org.apache.pinot.grigio.common.utils.IdealStateHelper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class KCUpdateLogRetentionManagerImplTest {
  private KCUpdateLogRetentionManagerImpl kcUpdateLogRetentionManager;
  private IdealStateHelper mockIdealStateHelper;
  private UpdateLogStorageProvider mockUpdateLogStorageProvider;
  private Map<String, Map<String, String>> table1SegmentMap;
  private Map<String, Map<String, String>> table2SegmentMap;

  @BeforeMethod
  public void setUp() {
    table1SegmentMap = new HashMap<>();

    table1SegmentMap.put("table1__0__10__20191027T2041Z",
        ImmutableMap.of("server1", "ONLINE", "server2", "ONLINE"));
    table1SegmentMap.put("table1__0__11__20191028T2041Z",
        ImmutableMap.of("server1", "ONLINE", "server3", "ONLINE"));
    table1SegmentMap.put("table1__1__10__20191027T2041Z",
        ImmutableMap.of("server1", "ONLINE", "server2", "ONLINE"));
    table1SegmentMap.put("table1__2__10__20191027T2041Z",
        ImmutableMap.of("server2", "ONLINE", "server3", "ONLINE"));

    table2SegmentMap = new HashMap<>();
    table2SegmentMap.put("table2__0__10__20191027T2041Z",
        ImmutableMap.of("server1", "ONLINE", "server2", "ONLINE"));

    mockIdealStateHelper = mock(IdealStateHelper.class);
    when(mockIdealStateHelper.getSegmentToInstanceMap("table1")).thenReturn(table1SegmentMap);
    when(mockIdealStateHelper.getSegmentToInstanceMap("table2")).thenReturn(table1SegmentMap);

    mockUpdateLogStorageProvider = mock(UpdateLogStorageProvider.class);
    kcUpdateLogRetentionManager = new KCUpdateLogRetentionManagerImpl(mockIdealStateHelper, mockUpdateLogStorageProvider,
        "kc1");
  }

  @Test
  public void testOnIdealStateChange() throws InterruptedException {
    IdealState table1MockIdealState = getMockIdealState("table1_REALTIME",
        ImmutableList.of("seg1", "host1"), ImmutableList.of("seg1", "host2"));

    IdealState table2MockIdealState = getMockIdealState("table2_REALTIME",
        ImmutableList.of("seg1", "host1"), ImmutableList.of("seg1", "host2"));

    UpdateLogTableRetentionManager table1RetentionManager = kcUpdateLogRetentionManager.
        getRetentionManagerForTable("table1_REALTIME");
    UpdateLogTableRetentionManager table2RetentionManager = kcUpdateLogRetentionManager.
        getRetentionManagerForTable("table2_REALTIME");

    verify(mockIdealStateHelper, times(1)).getSegmentToInstanceMap("table1_REALTIME");
    verify(mockIdealStateHelper, times(1)).getSegmentToInstanceMap("table2_REALTIME");

    kcUpdateLogRetentionManager.onIdealStateChange(ImmutableList.of(table1MockIdealState), mock(NotificationContext.class));
    verify(mockIdealStateHelper, times(2)).getSegmentToInstanceMap("table1_REALTIME");
    verify(mockIdealStateHelper, times(1)).getSegmentToInstanceMap("table2_REALTIME");

    // table1 idealstate is the same as last time
    kcUpdateLogRetentionManager.onIdealStateChange(ImmutableList.of(table1MockIdealState, table2MockIdealState), mock(NotificationContext.class));
    verify(mockIdealStateHelper, times(2)).getSegmentToInstanceMap("table1_REALTIME");
    verify(mockIdealStateHelper, times(2)).getSegmentToInstanceMap("table2_REALTIME");

    kcUpdateLogRetentionManager.onIdealStateChange(ImmutableList.of(
        getMockIdealState("table1_REALTIME", ImmutableList.of("seg1", "host1"), ImmutableList.of("seg2", "host2")),
        getMockIdealState("table2_REALTIME", ImmutableList.of("seg1", "host1"), ImmutableList.of("seg2", "host2"))
        ), mock(NotificationContext.class));
    verify(mockIdealStateHelper, times(3)).getSegmentToInstanceMap("table1_REALTIME");
    verify(mockIdealStateHelper, times(3)).getSegmentToInstanceMap("table2_REALTIME");

  }

  private IdealState getMockIdealState(String resourceName, List<String>... partitionInstance) {
    IdealState mockIdealState = new IdealState(resourceName);
    mockIdealState.enable(true);
    mockIdealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    Arrays.stream(partitionInstance).forEach(partition -> mockIdealState.setPartitionState(partition.get(0), partition.get(1), "ONLINE"));
    return mockIdealState;
  }
}
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
package org.apache.pinot.grigio.common.utils;

import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IdealStateHelperTest {

  private HelixAdmin mockAmin;
  private IdealStateHelper idealStateHelper;
  private IdealState mockIdealState;

  @BeforeClass
  public void init() {
    mockIdealState = new IdealState("resource");
    mockIdealState.enable(true);
    mockIdealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    mockIdealState.setPartitionState("seg1", "host1", "ONLINE");
    mockIdealState.setPartitionState("seg1", "host2", "ONLINE");
    mockIdealState.setPartitionState("seg2", "host2", "ONLINE");
    mockIdealState.setPartitionState("seg2", "host3", "OFFLINE");

    mockAmin = mock(HelixAdmin.class);
    when(mockAmin.getResourceIdealState(anyString(), anyString())).thenReturn(mockIdealState);
    idealStateHelper = new IdealStateHelper(mockAmin, "clusterName");
  }

  @Test
  public void testGetAllSegmentsForTable() {
    Set<String> segments = idealStateHelper.getAllSegmentsForTable("table");
    Assert.assertEquals(segments.size(), 2);
    Assert.assertTrue(segments.contains("seg1"));
    Assert.assertTrue(segments.contains("seg2"));
  }

  @Test
  public void testGetSegmentToInstanceMap() {
    Map<String, Map<String, String>> resultMap = idealStateHelper.getSegmentToInstanceMap("table");
    Assert.assertEquals(resultMap.size(), 2);
    Assert.assertEquals(resultMap.get("seg1").size(), 2);
    Assert.assertEquals(resultMap.get("seg1").get("host1"), "ONLINE");
    Assert.assertEquals(resultMap.get("seg1").get("host2"), "ONLINE");
    Assert.assertEquals(resultMap.get("seg2").size(), 2);
    Assert.assertEquals(resultMap.get("seg2").get("host2"), "ONLINE");
    Assert.assertEquals(resultMap.get("seg2").get("host3"), "OFFLINE");
  }
}
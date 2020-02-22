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

import com.google.common.collect.ImmutableMap;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogStorageProvider;
import org.apache.pinot.grigio.common.utils.IdealStateHelper;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KCUpdateLogTableRetentionManagerImplTest {
  private IdealStateHelper mockIdealStateHelper;
  private UpdateLogStorageProvider mockStorageProvider;
  private Map<String, Map<String, String>> segmentsInstanceMap;
  private Set<String> updateLogStorageSegments;
  private KCUpdateLogTableRetentionManagerImpl updateLogTableRetentionManager;

  @BeforeMethod
  public void setUp() throws IOException {
    segmentsInstanceMap = new HashMap<>();

    segmentsInstanceMap.put("table__0__10__20191027T2041Z",
        ImmutableMap.of("server1", "ONLINE", "server2", "ONLINE"));
    segmentsInstanceMap.put("table__0__11__20191028T2041Z",
        ImmutableMap.of("server1", "ONLINE", "server3", "ONLINE"));
    segmentsInstanceMap.put("table__1__10__20191027T2041Z",
        ImmutableMap.of("server1", "ONLINE", "server2", "ONLINE"));
    segmentsInstanceMap.put("table__2__10__20191027T2041Z",
        ImmutableMap.of("server2", "ONLINE", "server3", "ONLINE"));

    updateLogStorageSegments = new HashSet<>();
    updateLogStorageSegments.add("table__0__10__20191027T2041Z");
    updateLogStorageSegments.add("table__0__11__20191028T2041Z");
    updateLogStorageSegments.add("table__1__10__20191027T2041Z");
    updateLogStorageSegments.add("table__2__10__20191027T2041Z");

    mockIdealStateHelper = mock(IdealStateHelper.class);
    when(mockIdealStateHelper.getSegmentToInstanceMap("table")).thenReturn(segmentsInstanceMap);

    mockStorageProvider = mock(UpdateLogStorageProvider.class);
    when(mockStorageProvider.getAllSegments(anyString())).thenReturn(updateLogStorageSegments);

  }

  @Test
  public void testInit() throws IOException {
    updateLogStorageSegments.add("table__3__10__20191027T2041Z");
    updateLogTableRetentionManager = new KCUpdateLogTableRetentionManagerImpl(mockIdealStateHelper, "table",
        mockStorageProvider);

    // verify that we update helix and performed deletion for expired segments
    verify(mockStorageProvider, times(1))
        .removeSegment("table", "table__3__10__20191027T2041Z");
    verify(mockIdealStateHelper, times(1)).getSegmentToInstanceMap("table");
  }

  @Test
  public void testShouldIngestForSegment() throws IOException {
    updateLogTableRetentionManager = new KCUpdateLogTableRetentionManagerImpl(mockIdealStateHelper, "table",
        mockStorageProvider);

    // test the happy paths
    Assert.assertTrue(updateLogTableRetentionManager.shouldIngestForSegment("table__0__10__20191027T2041Z"));
    Assert.assertTrue(updateLogTableRetentionManager.shouldIngestForSegment("table__0__11__20191028T2041Z"));
    Assert.assertTrue(updateLogTableRetentionManager.shouldIngestForSegment("table__1__10__20191027T2041Z"));
    Assert.assertTrue(updateLogTableRetentionManager.shouldIngestForSegment("table__2__10__20191027T2041Z"));
    verify(mockIdealStateHelper, times(1)).getSegmentToInstanceMap("table");

    // test for segments that has lower seq
    Assert.assertFalse(updateLogTableRetentionManager.shouldIngestForSegment("table__1__5__20191027T2041Z"));
    verify(mockIdealStateHelper, times(1)).getSegmentToInstanceMap("table");

    //test for segment that are older and not in existing segment list
    Assert.assertFalse(updateLogTableRetentionManager.shouldIngestForSegment("table__1__20__20191027T2041Z"));
    verify(mockIdealStateHelper, times(1)).getSegmentToInstanceMap("table");

    //test for older timestamp but higher seq
    Assert.assertFalse(updateLogTableRetentionManager.shouldIngestForSegment("table__0__20__20191027T2041Z"));
    verify(mockIdealStateHelper, times(1)).getSegmentToInstanceMap("table");

    // test for newer segments, and there is update in newer ideal state
    segmentsInstanceMap.put("table__0__12__20191029T2041Z",
        ImmutableMap.of("server1", "ONLINE", "server3", "ONLINE"));
    Assert.assertTrue(updateLogTableRetentionManager.shouldIngestForSegment("table__0__12__20191029T2041Z"));
    verify(mockIdealStateHelper, times(2)).getSegmentToInstanceMap("table");

    // test for newer segments and there is no update in newer ideal state
    Assert.assertFalse(updateLogTableRetentionManager.shouldIngestForSegment("table__0__13__20191029T2141Z"));
    verify(mockIdealStateHelper, times(3)).getSegmentToInstanceMap("table");

    // multiple attempts trying to fetch for segment should not recheck the ideal state often
    Assert.assertFalse(updateLogTableRetentionManager.shouldIngestForSegment("table__0__13__20191029T2141Z"));
    verify(mockIdealStateHelper, times(3)).getSegmentToInstanceMap("table");

    // for unknown partition, we should also refresh data
    Assert.assertFalse(updateLogTableRetentionManager.shouldIngestForSegment("table__2__13__20191029T2141Z"));
    verify(mockIdealStateHelper, times(4)).getSegmentToInstanceMap("table");

    Assert.assertFalse(updateLogTableRetentionManager.shouldIngestForSegment("table__3__13__20191029T2141Z"));
    verify(mockIdealStateHelper, times(5)).getSegmentToInstanceMap("table");

  }

  @Test
  public void testNotifySegmentsChange() throws IOException {
    updateLogTableRetentionManager = new KCUpdateLogTableRetentionManagerImpl(mockIdealStateHelper, "table",
        mockStorageProvider);
    verify(mockIdealStateHelper, times(1)).getSegmentToInstanceMap("table");
    updateLogTableRetentionManager.notifySegmentsChange();
    verify(mockIdealStateHelper, times(2)).getSegmentToInstanceMap("table");
    verify(mockStorageProvider, never()).removeSegment(anyString(), anyString());

    segmentsInstanceMap.remove("table__0__10__20191027T2041Z");
    updateLogTableRetentionManager.notifySegmentsChange();
    verify(mockIdealStateHelper, times(3)).getSegmentToInstanceMap("table");
    verify(mockStorageProvider, times(1)).removeSegment("table", "table__0__10__20191027T2041Z");
  }
}
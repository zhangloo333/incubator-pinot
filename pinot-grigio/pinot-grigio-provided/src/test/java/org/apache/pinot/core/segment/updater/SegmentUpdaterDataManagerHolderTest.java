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
package org.apache.pinot.core.segment.updater;

import com.google.common.collect.ImmutableSet;
import org.apache.pinot.core.data.manager.upsert.DataManagerCallback;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Set;

import static org.mockito.Mockito.mock;

public class SegmentUpdaterDataManagerHolderTest {
  private SegmentUpdaterDataManagerHolder emptyHolder;
  private SegmentUpdaterDataManagerHolder dataManagerHolder;

  private DataManagerCallback dummyManager1;
  private DataManagerCallback dummyManager2;
  private DataManagerCallback dummyManager3;

  @BeforeMethod
  public void setUp() {
    emptyHolder = new SegmentUpdaterDataManagerHolder();
    dataManagerHolder = new SegmentUpdaterDataManagerHolder();
    dummyManager1 = mock(DataManagerCallback.class);
    dummyManager2 = mock(DataManagerCallback.class);
    dummyManager3 = mock(DataManagerCallback.class);
    dataManagerHolder.addDataManager("table", "segment1", dummyManager1);
    dataManagerHolder.addDataManager("table", "segment2", dummyManager2);
    dataManagerHolder.addDataManager("table2", "segment3", dummyManager3);
  }

  @Test
  public void testGetAllTables() {
    Set<String> tables = dataManagerHolder.getAllTables();
    ensureSetEqual(tables, ImmutableSet.of("table", "table2"));

    Assert.assertEquals(emptyHolder.getAllTables().size(), 0);
  }

  @Test
  public void testHasTable() {
    Assert.assertFalse(emptyHolder.hasTable("table"));

    Assert.assertTrue(dataManagerHolder.hasTable("table"));
    Assert.assertTrue(dataManagerHolder.hasTable("table2"));
    Assert.assertFalse(dataManagerHolder.hasTable("table3"));
  }

  @Test
  public void testGetDataManagers() {
    Set<DataManagerCallback> dataManagers = dataManagerHolder.getDataManagers("table", "segment1");
    ensureSetEqual(dataManagers, ImmutableSet.of(dummyManager1));

    dataManagers = dataManagerHolder.getDataManagers("table", "segment2");
    ensureSetEqual(dataManagers, ImmutableSet.of(dummyManager2));

    dataManagers = dataManagerHolder.getDataManagers("table2", "segment3");
    ensureSetEqual(dataManagers, ImmutableSet.of(dummyManager3));

    // non exist tables/segments
    dataManagers = dataManagerHolder.getDataManagers("table2", "segment1");
    ensureSetEqual(dataManagers, ImmutableSet.of());

    dataManagers = dataManagerHolder.getDataManagers("table3", "segment1");
    ensureSetEqual(dataManagers, ImmutableSet.of());
  }

  @Test
  public void testAddDataManager() {
    DataManagerCallback dummyManager4 = mock(DataManagerCallback.class);
    DataManagerCallback dummyManager5 = mock(DataManagerCallback.class);
    DataManagerCallback dummyManager6 = mock(DataManagerCallback.class);
    dataManagerHolder.addDataManager("table", "segment1", dummyManager4);
    dataManagerHolder.addDataManager("table", "segment2", dummyManager5);
    dataManagerHolder.addDataManager("table2", "segment1", dummyManager6);

    Set<DataManagerCallback> tableSegmentDMs = dataManagerHolder.getDataManagers("table", "segment1");
    ensureSetEqual(tableSegmentDMs, ImmutableSet.of(dummyManager1, dummyManager4));

    tableSegmentDMs = dataManagerHolder.getDataManagers("table", "segment2");
    ensureSetEqual(tableSegmentDMs, ImmutableSet.of(dummyManager2, dummyManager5));

    tableSegmentDMs = dataManagerHolder.getDataManagers("table2", "segment1");
    ensureSetEqual(tableSegmentDMs, ImmutableSet.of(dummyManager6));
  }

  @Test
  public void testRemoveDataManager() {
    DataManagerCallback dummyManager4 = mock(DataManagerCallback.class);
    DataManagerCallback dummyManager5 = mock(DataManagerCallback.class);
    DataManagerCallback dummyManager6 = mock(DataManagerCallback.class);
    dataManagerHolder.addDataManager("table", "segment1", dummyManager4);
    dataManagerHolder.addDataManager("table", "segment2", dummyManager5);
    dataManagerHolder.addDataManager("table2", "segment1", dummyManager6);
    Set<DataManagerCallback> tableSegmentDMs;

    // start deleting
    dataManagerHolder.removeDataManager("table", "segment1", dummyManager1);
    dataManagerHolder.removeDataManager("table", "segment1", dummyManager2);
    dataManagerHolder.removeDataManager("table", "segment", dummyManager2);
    tableSegmentDMs = dataManagerHolder.getDataManagers("table", "segment1");
    ensureSetEqual(tableSegmentDMs, ImmutableSet.of(dummyManager4));

    // delete all segment
    dataManagerHolder.removeDataManager("table", "segment1", dummyManager4);
    tableSegmentDMs = dataManagerHolder.getDataManagers("table", "segment1");
    ensureSetEqual(tableSegmentDMs, ImmutableSet.of());

    // delete some more
    dataManagerHolder.removeDataManager("table", "segment1", dummyManager4);
    tableSegmentDMs = dataManagerHolder.getDataManagers("table", "segment1");
    ensureSetEqual(tableSegmentDMs, ImmutableSet.of());

    // add some back and delete
    dataManagerHolder.addDataManager("table", "segment1", dummyManager4);
    ensureSetEqual(dataManagerHolder.getDataManagers("table", "segment1"), ImmutableSet.of(dummyManager4));

    dataManagerHolder.removeDataManager("table", "segment1", dummyManager4);
    ensureSetEqual(dataManagerHolder.getDataManagers("table", "segment1"), ImmutableSet.of());
  }

  @Test
  public void testRemoveAllDataManagerForSegment() {
    DataManagerCallback dummyManager4 = mock(DataManagerCallback.class);
    DataManagerCallback dummyManager5 = mock(DataManagerCallback.class);
    DataManagerCallback dummyManager6 = mock(DataManagerCallback.class);
    dataManagerHolder.addDataManager("table", "segment1", dummyManager4);
    dataManagerHolder.addDataManager("table", "segment2", dummyManager5);
    dataManagerHolder.addDataManager("table2", "segment1", dummyManager6);

    boolean result = dataManagerHolder.removeAllDataManagerForSegment("table", "segment1");
    ensureSetEqual(dataManagerHolder.getDataManagers("table", "segment1"), ImmutableSet.of());
    Assert.assertTrue(result);

    result = dataManagerHolder.removeAllDataManagerForSegment("table", "segment1");
    ensureSetEqual(dataManagerHolder.getDataManagers("table", "segment1"), ImmutableSet.of());
    Assert.assertFalse(result);

    result = dataManagerHolder.removeAllDataManagerForSegment("table3", "segment1");
    Assert.assertFalse(result);
  }

  @Test
  public void testMaybeRemoveTable() {

    Assert.assertFalse(dataManagerHolder.maybeRemoveTable("table"));
    Assert.assertFalse(dataManagerHolder.maybeRemoveTable("table2"));
    Assert.assertFalse(dataManagerHolder.maybeRemoveTable("table4"));

    dataManagerHolder.removeAllDataManagerForSegment("table2", "segment3");
    Assert.assertTrue(dataManagerHolder.maybeRemoveTable("table2"));

    dataManagerHolder.removeAllDataManagerForSegment("table", "segment1");
    Assert.assertFalse(dataManagerHolder.maybeRemoveTable("table"));
    dataManagerHolder.removeAllDataManagerForSegment("table", "segment2");
    Assert.assertTrue(dataManagerHolder.maybeRemoveTable("table"));
  }

  private <T> void ensureSetEqual(Set<T> set1, Set<T> set2) {
    Assert.assertEquals(set2.size(), set1.size());
    for (T o: set1) {
      Assert.assertTrue(set2.contains(o));
    }
  }
}
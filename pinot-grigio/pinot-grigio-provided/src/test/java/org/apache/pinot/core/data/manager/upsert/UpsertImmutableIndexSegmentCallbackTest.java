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
package org.apache.pinot.core.data.manager.upsert;

import org.apache.pinot.core.segment.updater.UpsertWatermarkManager;
import org.apache.pinot.core.segment.virtualcolumn.mutable.VirtualColumnLongValueReaderWriter;
import org.apache.pinot.grigio.common.messages.LogEventType;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogEntry;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogEntrySet;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogStorageProvider;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class UpsertImmutableIndexSegmentCallbackTest {
  UpdateLogStorageProvider _mockProvider;
  UpsertWatermarkManager _mockUpsertWatermarkManager;
  List<VirtualColumnLongValueReaderWriter> _readerWriters = new ArrayList<>();

  @BeforeMethod
  public void init() {
    _mockProvider = mock(UpdateLogStorageProvider.class);
    _mockUpsertWatermarkManager = mock(UpsertWatermarkManager.class);
  }

  @Test
  public void testInitVirtualColumn() throws IOException {
    long start = System.currentTimeMillis();
    long minOffset = 5000_000l;
    int totalDocs = 5_000_000;
    _readerWriters.add(new VirtualColumnLongValueReaderWriter(totalDocs, false) {
      @Override
      public boolean update(int docId, long value, LogEventType eventType) {
        if (eventType == LogEventType.INSERT) {
          updateValue(docId, value);
          return true;
        }
        return false;
      }

      @Override
      public boolean update(int docId, int value, LogEventType eventType) {
        return update(docId, value, eventType);
      }
    });
    _readerWriters.add(new VirtualColumnLongValueReaderWriter(totalDocs, false) {
      @Override
      public boolean update(int docId, long value, LogEventType eventType) {
        if (eventType == LogEventType.DELETE) {
          updateValue(docId, value);
          return true;
        }
        return false;
      }

      @Override
      public boolean update(int docId, int value, LogEventType eventType) {
        return update(docId, value, eventType);
      }
    });
    int[] offsetToDocId = new int[totalDocs];
    for (int i = 0; i < totalDocs; i++) {
      offsetToDocId[i] = i;
    }
    List<UpdateLogEntry> updateLogEntries = new ArrayList<>(totalDocs * 2);
    for (int i = 0; i < totalDocs; i++) {
      updateLogEntries.add(new UpdateLogEntry(minOffset + i, 50, LogEventType.INSERT, i%8));
      updateLogEntries.add(new UpdateLogEntry(minOffset + i, 100, LogEventType.DELETE, i%8));
    }
    UpdateLogEntrySet entrySet = new UpdateLogEntrySet(null, 2) {
      @Override
      public Iterator<UpdateLogEntry> iterator() {
        return updateLogEntries.iterator();
      }
    };
    when(_mockProvider.getAllMessages(anyString(), anyString())).thenReturn(entrySet);
    System.out.println("run time for set up: " + (System.currentTimeMillis() - start));

    start = System.currentTimeMillis();

    UpsertImmutableIndexSegmentCallback callback = new UpsertImmutableIndexSegmentCallback();
    callback.init(_readerWriters, totalDocs, _mockUpsertWatermarkManager, _mockProvider, minOffset, offsetToDocId);
    callback.initVirtualColumn();

    long runtime = System.currentTimeMillis() - start;
    System.out.println("run time is " + runtime);
    // on regular developer laptop this should take less 1 second, but on integration server this might be longer
    Assert.assertTrue(runtime < 10_000L, "run time should be less than 10 second");

    VirtualColumnLongValueReaderWriter insertReaderWrite = _readerWriters.get(0);
    VirtualColumnLongValueReaderWriter deleteReaderWrite = _readerWriters.get(1);
    for (int i = 0; i < totalDocs; i++) {
      if (insertReaderWrite.getLong(i) != 50 || deleteReaderWrite.getLong(i) != 100) {
        System.out.println(String.format("position %d has value %d/%d", i, insertReaderWrite.getLong(i),
            deleteReaderWrite.getLong(i)));
        Assert.fail("no correct value");
      }
    }
  }
}
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
package org.apache.pinot.grigio.common.storageProvider;

import com.google.common.collect.ImmutableList;
import org.apache.pinot.grigio.common.messages.LogEventType;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SegmentUpdateLogStorageProviderTest {

  SegmentUpdateLogStorageProvider provider;
  List<UpdateLogEntry> inputDataList = ImmutableList.of(
      new UpdateLogEntry(1, 2, LogEventType.INSERT, 0),
      new UpdateLogEntry(2, 3, LogEventType.DELETE, 0),
      new UpdateLogEntry(4,10, LogEventType.DELETE, 0));

  @BeforeMethod
  public void setup() throws IOException {
    File file = File.createTempFile("test", "postFix");
    file.deleteOnExit();
    this.provider = new SegmentUpdateLogStorageProvider(file);
  }

  @Test
  public void testWriteAndReadData() throws IOException {
    List<UpdateLogEntry> logEntryList = provider.readAllMessagesFromFile();
    // new file should have no data
    Assert.assertEquals(logEntryList.size(), 0);
    provider.addData(inputDataList);

    SegmentUpdateLogStorageProvider provider1= new SegmentUpdateLogStorageProvider(provider._file);
    logEntryList = provider1.readAllMessagesFromFile();
    Assert.assertEquals(logEntryList.size(), inputDataList.size());
    Assert.assertEquals(logEntryList.get(0), inputDataList.get(0));
    Assert.assertEquals(logEntryList.get(1), inputDataList.get(1));
    Assert.assertEquals(logEntryList.get(2), inputDataList.get(2));
    provider.addData(inputDataList);
    Assert.assertEquals(provider.readAllMessagesFromFile().size(), inputDataList.size() * 2);
  }

  @Test
  public void testReadPartialData() throws IOException {
    provider.addData(inputDataList);
    // write some more data to channel and persist it
    provider._outputStream.write(new byte[]{1,2,3});
    provider._outputStream.flush();

    SegmentUpdateLogStorageProvider provider1 = new SegmentUpdateLogStorageProvider(provider._file);
    List<UpdateLogEntry> logEntryList = provider1.readAllMessagesFromFile();
    Assert.assertEquals(logEntryList.size(), inputDataList.size());
    Assert.assertEquals(logEntryList.get(0), inputDataList.get(0));
    Assert.assertEquals(logEntryList.get(1), inputDataList.get(1));
    Assert.assertEquals(logEntryList.get(2), inputDataList.get(2));
  }

  @Test
  public void testMultiThreadReadAndWrite() throws InterruptedException, IOException {

    ExecutorService service = Executors.newFixedThreadPool(2);
    final long writeIterationCount = 1000;
    final long readIterationCount = 100;
    List<Callable<Object>> tasks = new ArrayList<>();
    tasks.add(() -> {
      for (int i = 0; i < writeIterationCount; i++) {
        try {
          provider.addData(inputDataList);
        } catch (IOException e) {
          Assert.fail();
        }
      }
      return null;
    });
    tasks.add(() -> {
      for (int i = 0; i < readIterationCount; i++) {
        try {
          provider.readAllMessagesFromFile();
        } catch (IOException e) {
          Assert.fail();
        }
      }
      return null;
    });
    service.invokeAll(tasks);
    service.shutdownNow();
    List<UpdateLogEntry> updateLogEntries = provider.readAllMessagesFromFile();
    Assert.assertEquals(updateLogEntries.size(), writeIterationCount * inputDataList.size());
    for (int i = 0; i < writeIterationCount; i++) {
      Assert.assertEquals(updateLogEntries.get(i * inputDataList.size()), inputDataList.get(0));
      Assert.assertEquals(updateLogEntries.get(i * inputDataList.size() + 1), inputDataList.get(1));
      Assert.assertEquals(updateLogEntries.get(i * inputDataList.size() + 2), inputDataList.get(2));
    }

  }
}
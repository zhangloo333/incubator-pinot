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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SegmentUpdateLogStorageProviderTest {

  protected volatile UpdateLogEntry entryHolder;
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
    UpdateLogEntrySet logEntrySet = provider.readAllMessagesFromFile();
    // new file should have no data
    Assert.assertEquals(logEntrySet.size(), 0);
    provider.addData(inputDataList);

    SegmentUpdateLogStorageProvider provider1= new SegmentUpdateLogStorageProvider(provider._file);
    logEntrySet = provider1.readAllMessagesFromFile();
    Iterator<UpdateLogEntry> it = logEntrySet.iterator();
    Assert.assertEquals(logEntrySet.size(), inputDataList.size());
    Assert.assertEquals(it.next(), inputDataList.get(0));
    Assert.assertEquals(it.next(), inputDataList.get(1));
    Assert.assertEquals(it.next(), inputDataList.get(2));
    Assert.assertFalse(it.hasNext());
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
    UpdateLogEntrySet logEntrySet = provider1.readAllMessagesFromFile();
    Iterator<UpdateLogEntry> it = logEntrySet.iterator();
    Assert.assertEquals(logEntrySet.size(), inputDataList.size());
    Assert.assertEquals(it.next(), inputDataList.get(0));
    Assert.assertEquals(it.next(), inputDataList.get(1));
    Assert.assertEquals(it.next(), inputDataList.get(2));
    Assert.assertFalse(it.hasNext());
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
    UpdateLogEntrySet updateLogEntries = provider.readAllMessagesFromFile();
    Assert.assertEquals(updateLogEntries.size(), writeIterationCount * inputDataList.size());
    Iterator<UpdateLogEntry> it = updateLogEntries.iterator();
    for (int i = 0; i < writeIterationCount; i++) {
      Assert.assertEquals(it.next(), inputDataList.get(0));
      Assert.assertEquals(it.next(), inputDataList.get(1));
      Assert.assertEquals(it.next(), inputDataList.get(2));
    }
    Assert.assertFalse(it.hasNext());
  }

  @Test
  public void testReadMesssagePerf() throws IOException {
    int totalMessageCount = 5_000_000;
    // write a lot of data to file
    List<UpdateLogEntry> inputMessages = new ArrayList<>(totalMessageCount * 2);
    for (int i = 0; i < totalMessageCount; i++) {
      inputMessages.add(new UpdateLogEntry(i, 50, LogEventType.INSERT, i%8));
      inputMessages.add(new UpdateLogEntry(i, 100, LogEventType.DELETE, i%8));
    }
    long start = System.currentTimeMillis();
    provider.addData(inputMessages);
    System.out.println("write data takes ms: " + (System.currentTimeMillis() - start));

    // load data from file to temp object, we don't measure this performance as it depends on disk/computer
    start = System.currentTimeMillis();
    UpdateLogEntrySet entrySet = provider.readAllMessagesFromFile();
    long loadTime = System.currentTimeMillis() - start;
    System.out.println("load data takes ms: " + loadTime);
    Assert.assertTrue(entrySet.size() == totalMessageCount * 2);

    // old implementation where we hold the data in array list will take 1000 - 2000 seconds for the data loading
    // using iterator (current implementation) should make this code finished within 300 - 600 ms.
    // test accessing those object
    start = System.currentTimeMillis();
    for (UpdateLogEntry entry: entrySet) {
      // ensure we hold them in volatile member to force JVM allocate the object and
      // prevent JIT optimize this part of code away
      entryHolder = entry;
    }
    long readTime = System.currentTimeMillis() - start;
    Assert.assertTrue(readTime < 1_000L); // this should be relatively fast
    System.out.println("read data takes ms: " + readTime);
  }
}
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
package org.apache.pinot.opal.common.storageProvider;

import com.google.common.collect.ImmutableList;
import org.apache.pinot.opal.common.messages.LogEventType;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

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
  public void testAddAndReadData() throws IOException {
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
  }

  @Test
  public void testReadPartialData() throws IOException {
    provider.addData(inputDataList);
    // write some more data to channel and persist it
    provider._channel.write(ByteBuffer.wrap(new byte[]{1,2,3}));
    provider._channel.force(true);

    SegmentUpdateLogStorageProvider provider1 = new SegmentUpdateLogStorageProvider(provider._file);
    List<UpdateLogEntry> logEntryList = provider1.readAllMessagesFromFile();
    Assert.assertEquals(logEntryList.size(), inputDataList.size());
    Assert.assertEquals(logEntryList.get(0), inputDataList.get(0));
    Assert.assertEquals(logEntryList.get(1), inputDataList.get(1));
    Assert.assertEquals(logEntryList.get(2), inputDataList.get(2));
  }

}
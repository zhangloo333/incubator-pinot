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
import com.google.common.io.Files;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.opal.common.messages.LogEventType;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class UpdateLogStorageProviderTest {

  private Configuration conf;
  private File tempDir;

  @BeforeMethod
  public void setup() {
    conf = new PropertiesConfiguration();
    tempDir = Files.createTempDir();
    tempDir.deleteOnExit();
    conf.setProperty(UpdateLogStorageProvider.BASE_PATH_CONF_KEY, tempDir.getAbsolutePath());

    UpdateLogStorageProvider._instance = null;
    UpdateLogStorageProvider.init(conf);
  }

  @Test
  public void testAddSegment() throws IOException {
    UpdateLogStorageProvider provider = UpdateLogStorageProvider.getInstance();
    provider.addSegment("table", "segment");
    File segmentFile = new File(new File(tempDir, "table"), "segment");
    Assert.assertTrue(segmentFile.exists());
  }

  @Test
  public void testAddAndGetData() throws IOException {
    UpdateLogStorageProvider provider = UpdateLogStorageProvider.getInstance();
    List<UpdateLogEntry> inputDataList1 = ImmutableList.of(
        new UpdateLogEntry(1, 2, LogEventType.INSERT, 0),
        new UpdateLogEntry(2, 3, LogEventType.DELETE, 0),
        new UpdateLogEntry(4,10, LogEventType.DELETE, 0));
    List<UpdateLogEntry> inputDataList2 = ImmutableList.of(
        new UpdateLogEntry(10, 11, LogEventType.INSERT, 0));
    List<UpdateLogEntry> inputDataList3 = ImmutableList.of(
        new UpdateLogEntry(100, 110, LogEventType.DELETE, 0));
    List<UpdateLogEntry> inputDataList4 = ImmutableList.of();

    provider.addSegment("table", "segment");
    provider.addSegment("table2", "segment");

    provider.addDataToFile("table", "segment", inputDataList1);
    provider.addDataToFile("table", "segment1", inputDataList2);
    provider.addDataToFile("table2", "segment", inputDataList3);
    provider.addDataToFile("table2", "segment1", inputDataList4);

    Assert.assertEquals(provider.getAllMessages("table", "segment"), inputDataList1);
    Assert.assertEquals(provider.getAllMessages("table", "segment1"), inputDataList2);
    Assert.assertEquals(provider.getAllMessages("table2", "segment"), inputDataList3);
    Assert.assertEquals(provider.getAllMessages("table2", "segment1"), inputDataList4);
    Assert.assertEquals(provider.getAllMessages("table2", "segment2"), inputDataList4);
  }

  @Test
  public void testRemoveSegment() throws IOException {
    UpdateLogStorageProvider provider = UpdateLogStorageProvider.getInstance();
    provider.addSegment("table", "segment");
    provider.addSegment("table", "segment1");
    provider.addSegment("table1", "segment1");

    provider.removeSegment("table", "segment");

    Assert.assertFalse(new File(new File(tempDir, "table"), "segment").exists());
    Assert.assertTrue(new File(new File(tempDir, "table"), "segment1").exists());
    Assert.assertTrue(new File(new File(tempDir, "table1"), "segment1").exists());

  }
}
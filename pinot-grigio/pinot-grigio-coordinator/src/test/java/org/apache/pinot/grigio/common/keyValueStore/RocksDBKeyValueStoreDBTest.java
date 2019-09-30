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
package org.apache.pinot.grigio.common.keyValueStore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.grigio.common.messages.KeyCoordinatorMessageContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;


public class RocksDBKeyValueStoreDBTest {

  private File tempDir;
  private Configuration configuration;

  @BeforeClass
  public void init() {
    tempDir = Files.createTempDir();
    tempDir.deleteOnExit();
    configuration = new PropertiesConfiguration();
    configuration.setProperty("rocksdb.database.dir", tempDir.getAbsolutePath());

  }

  @Test
  public void testGetTable() throws IOException {
    File table1Folder = new File(tempDir, "abc");
    File table2Folder = new File(tempDir, "dec");

    RocksDBKeyValueStoreDB db = new RocksDBKeyValueStoreDB();
    db.init(configuration);
    KeyValueStoreTable<ByteArrayWrapper, KeyCoordinatorMessageContext> abcTable = db.getTable("abc");

    // only create the abc table but not the other
    Assert.assertTrue(table1Folder.exists());
    Assert.assertTrue(table1Folder.isDirectory());
    Assert.assertFalse(table2Folder.exists());

    KeyValueStoreTable<ByteArrayWrapper, KeyCoordinatorMessageContext> decTable = db.getTable("dec");

    // dec table is also created
    Assert.assertTrue(table2Folder.exists());
    Assert.assertTrue(table2Folder.isDirectory());

    // test putting & getting data from two different tables and ensure they store correct values
    ByteArrayWrapper key = new ByteArrayWrapper(new byte[]{1, 3, 4});
    KeyCoordinatorMessageContext value1 = new KeyCoordinatorMessageContext("a", 1, 2);
    KeyCoordinatorMessageContext value2 = new KeyCoordinatorMessageContext("b", 2, 3);

    abcTable.multiPut(ImmutableMap.of(key, value1));
    decTable.multiPut(ImmutableMap.of(key, value2));

    Map<ByteArrayWrapper, KeyCoordinatorMessageContext> abcResult = abcTable.multiGet(ImmutableList.of(key));
    Map<ByteArrayWrapper, KeyCoordinatorMessageContext> decResult = decTable.multiGet(ImmutableList.of(key));

    // make sure each table has their own values store in it and they don't interfere with each other
    Assert.assertEquals(abcResult.size(), 1);
    Assert.assertEquals(decResult.size(), 1);
    Assert.assertEquals(abcResult.get(key), value1);
    Assert.assertEquals(decResult.get(key), value2);
  }
}
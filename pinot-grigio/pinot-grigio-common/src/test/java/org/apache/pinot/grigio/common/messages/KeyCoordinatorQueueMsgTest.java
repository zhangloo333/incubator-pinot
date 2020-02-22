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
package org.apache.pinot.grigio.common.messages;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

import static org.testng.Assert.*;

public class KeyCoordinatorQueueMsgTest {

  private KeyCoordinatorQueueMsg msg;
  private KeyCoordinatorQueueMsg versionMsg;
  private String key = "abc";
  private byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
  private String segmentName = "table_name__15__9__20190718T1930Z";
  private long offset = 1000;
  private long timestamp = 1000;
  private long version = 100;

  @BeforeClass
  public void setup() {
    msg = new KeyCoordinatorQueueMsg(keyBytes, segmentName, offset, timestamp);
    versionMsg = new KeyCoordinatorQueueMsg(version);
  }

  @Test
  public void testIsVersionMessage() {
    assertFalse(msg.isVersionMessage());
    assertTrue(versionMsg.isVersionMessage());
  }

  @Test
  public void testGetKey() {
    assertEquals(msg.getKey(), keyBytes);
  }

  @Test
  public void testGetSegmentName() {
    assertEquals(msg.getSegmentName(), segmentName);
  }

  @Test
  public void testGetTimestamp() {
    assertEquals(msg.getTimestamp(), timestamp);
  }

  @Test
  public void testGetKafkaOffset() {
    assertEquals(msg.getKafkaOffset(), offset);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testGetVersionFails() {
    msg.getVersion();
  }

  @Test
  public void testGetPinotTable() {
    assertEquals(msg.getPinotTableName(), "table_name");
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testGetKeyFails() {
    versionMsg.getKey();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testGetSegmentNameFails() {
    versionMsg.getSegmentName();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testGetTimestampFails() {
    versionMsg.getTimestamp();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testGetKafkaOffsetFails() {
    versionMsg.getKafkaOffset();
  }

  @Test
  public void testGetVersion() {
    assertEquals(version, versionMsg.getVersion());
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testGetPinotTableFails() {
    versionMsg.getPinotTableName();
  }
}
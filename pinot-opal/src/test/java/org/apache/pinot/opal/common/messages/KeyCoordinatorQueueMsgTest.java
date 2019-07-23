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
package org.apache.pinot.opal.common.messages;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

import static org.testng.Assert.*;

public class KeyCoordinatorQueueMsgTest {

  private KeyCoordinatorQueueMsg msg;
  private String key = "abc";
  private byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
  private String segmentName = "table_name__15__9__20190718T1930Z";
  private long offset = 1000;
  private long timestamp = 1000;

  @BeforeClass
  public void setup() {
    msg = new KeyCoordinatorQueueMsg(keyBytes, segmentName, offset, timestamp);
  }

  @Test
  public void testGetKey() {
    assertEquals(keyBytes, msg.getKey());
  }

  @Test
  public void testGetContext() {
    assertEquals(new KeyCoordinatorMessageContext(segmentName, offset, timestamp), msg.getContext());
  }

  @Test
  public void testGetPinotTable() {
    assertEquals("table_name", msg.getPinotTableName());
  }
}
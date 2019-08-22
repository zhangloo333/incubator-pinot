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

import org.apache.pinot.opal.common.messages.LogEventType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

public class UpdateLogEntryTest {

  @Test
  public void testGets() {
    UpdateLogEntry entry = new UpdateLogEntry(123, 12, LogEventType.INSERT, 0);
    Assert.assertEquals(entry.getOffset(), 123);
    Assert.assertEquals(entry.getValue(), 12);
    Assert.assertEquals(entry.getType(), LogEventType.INSERT);
  }

  @Test
  public void testEquals() {
    UpdateLogEntry entry = new UpdateLogEntry(123, 12, LogEventType.INSERT, 0);
    Assert.assertEquals(entry, new UpdateLogEntry(123, 12, LogEventType.INSERT, 0));
    Assert.assertNotEquals(entry, new UpdateLogEntry(12, 12, LogEventType.INSERT, 0));
    Assert.assertNotEquals(entry, new UpdateLogEntry(123, 121, LogEventType.INSERT, 0));
    Assert.assertNotEquals(entry, new UpdateLogEntry(123, 12, LogEventType.DELETE, 0));
  }

  @Test
  public void testSerialDeserial() {
    ByteBuffer buffer = ByteBuffer.allocate(UpdateLogEntry.SIZE);
    UpdateLogEntry entry = new UpdateLogEntry(1, 2, LogEventType.INSERT, 0);
    entry.addEntryToBuffer(buffer);
    buffer.flip();
    Assert.assertEquals(UpdateLogEntry.fromBytesBuffer(buffer), entry);
  }
}
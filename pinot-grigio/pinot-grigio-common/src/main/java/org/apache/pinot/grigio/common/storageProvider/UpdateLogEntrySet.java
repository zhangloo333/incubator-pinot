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

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * class for holding the list of update logs we read from files
 * provide iterator interface for more efficient memory access
 */
public class UpdateLogEntrySet implements Iterable<UpdateLogEntry> {

  private final ByteBuffer _buffer;
  private final int _messageCount;
  private static final UpdateLogEntrySet EMPTY_LOG_ENTRY_SET = new UpdateLogEntrySet(ByteBuffer.allocate(0),
      0);

  public UpdateLogEntrySet(ByteBuffer buffer, int messageCount) {
    _buffer = buffer;
    _messageCount = messageCount;
  }

  public int size() {
    return _messageCount;
  }

  @Override
  public Iterator<UpdateLogEntry> iterator() {
    return new Iterator<UpdateLogEntry>() {
      @Override
      public boolean hasNext() {
        return _buffer != null && _buffer.hasRemaining();
      }

      @Override
      public UpdateLogEntry next() {
        if (!hasNext()) {
          throw new RuntimeException("no more entries in buffer");
        }
        return UpdateLogEntry.fromBytesBuffer(_buffer);
      }
    };
  }

  /**
   * helper method to create a default empty set in case of invalid/missing input
   * @return an empty entry set has no data
   */
  public static UpdateLogEntrySet getEmptySet() {
    return EMPTY_LOG_ENTRY_SET;
  }
}

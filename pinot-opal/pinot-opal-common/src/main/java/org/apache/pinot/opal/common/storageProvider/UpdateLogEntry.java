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

import org.apache.pinot.opal.common.messages.LogCoordinatorMessage;
import org.apache.pinot.opal.common.messages.LogEventType;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * class for the local upsert update log entry,
 * offset: the offset of the origin message location
 * value: the value of this record should be updated to
 * type: the type of the message we are operating
 */
public class UpdateLogEntry implements Serializable {
  public static final int SIZE = Long.BYTES * 2 + Integer.BYTES;
  private final long _offset;
  private final long _value;
  private final LogEventType _type;

  public UpdateLogEntry(long offset, long value, LogEventType type) {
    _offset = offset;
    _value = value;
    _type = type;
  }

  public UpdateLogEntry(LogCoordinatorMessage logCoordinatorMessage) {
    this(logCoordinatorMessage.getKafkaOffset(), logCoordinatorMessage.getValue(), logCoordinatorMessage.getUpdateEventType());
  }

  public long getOffset() {
    return _offset;
  }

  public long getValue() {
    return _value;
  }

  public LogEventType getType() {
    return _type;
  }

  public void addEntryToBuffer(ByteBuffer buffer) {
    buffer.putLong(_offset);
    buffer.putLong(_value);
    buffer.putInt(_type.getUUID());
  }

  public static UpdateLogEntry fromBytesBuffer(ByteBuffer buffer) {
    return new UpdateLogEntry(buffer.getLong(), buffer.getLong(), LogEventType.getEventType(buffer.getInt()));
  }


  public String toString() {
    return "logEventEntry: offset " + _offset + " value " + _value + " type " + _type.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UpdateLogEntry logEntry = (UpdateLogEntry) o;
    return _offset == logEntry._offset &&
        _value == logEntry._value &&
        _type == logEntry._type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_offset, _value, _type);
  }
}

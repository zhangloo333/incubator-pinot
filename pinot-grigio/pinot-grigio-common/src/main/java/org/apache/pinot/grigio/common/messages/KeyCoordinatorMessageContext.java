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

import org.apache.commons.lang.SerializationUtils;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/**
 * this class is used as the value stored in key-value store
 * following field:
 * segmentName: the name of the segment that contains this record
 * timestamp: the timestamp of the current message (from pinot raw data)
 * kafkaOffset: the kafka offset of this message
 */
public class KeyCoordinatorMessageContext implements Serializable {

  private final String _segmentName;
  private final long _timestamp;
  private final long _kafkaOffset;

  public KeyCoordinatorMessageContext(String segmentName, long timestamp, long kafkaOffset) {
    _segmentName = segmentName;
    _timestamp = timestamp;
    _kafkaOffset = kafkaOffset;
  }

  public byte[] toBytes() {
    return SerializationUtils.serialize(this);
  }

  public static Optional<KeyCoordinatorMessageContext> fromBytes(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return Optional.empty();
    }
    return Optional.ofNullable((KeyCoordinatorMessageContext) SerializationUtils.deserialize(bytes));
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public long getTimestamp() {
    return _timestamp;
  }

  public long getKafkaOffset() {
    return _kafkaOffset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    KeyCoordinatorMessageContext that = (KeyCoordinatorMessageContext) o;
    return _timestamp == that._timestamp &&
        _kafkaOffset == that._kafkaOffset &&
        Objects.equals(_segmentName, that._segmentName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_segmentName, _timestamp, _kafkaOffset);
  }

}

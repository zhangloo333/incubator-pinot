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

import com.google.common.base.Preconditions;
import org.apache.pinot.common.utils.LLCSegmentName;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

/**
 * class used for the message pass from server to key coordinator
 * it contains the following field: table name, primary key and other metadata (in the _context field)
 *
 * There are two kinds of messages: version message and regular message. Version messages contains only
 * version field, while regular message contains all other fields but not version field.
 */
public class KeyCoordinatorQueueMsg implements Serializable {
  private static final byte[] KEY_PLACEHOLDER = new byte[0];  // placeholder for key field for version messages
  private static final String SEGMENT_NAME_PLACEHOLDER = "";  // placeholder for segmentName field for version messages
  private static final long TIMESTAMP_PLACEHOLDER = -1;  // placeholder for timestamp field for version messages
  private static final long KAFKA_OFFSET_PLACEHOLDER = -1;  // placeholder for kafka offset field for version messages
  private static final long VERSION_PLACEHOLDER = -1;  // placeholder for version field for regular messages

  private final byte[] _key;
  private final String _segmentName;
  private final long _kafkaOffset;
  private final long _timestamp;
  private final long _version;  // positive number when it is a version message, VERSION_PLACEHOLDER (-1) otherwise.

  /**
   * Constructor for regular messages
   */
  public KeyCoordinatorQueueMsg(byte[] key, String segmentName, long timestamp, long kafkaOffset) {
    this._key = key;
    this._segmentName = segmentName;
    this._timestamp = timestamp;
    this._kafkaOffset = kafkaOffset;
    this._version = VERSION_PLACEHOLDER;
  }

  /**
   * Constructor for version messages
   */
  public KeyCoordinatorQueueMsg(long version) {
    this._key = KEY_PLACEHOLDER;
    this._segmentName = SEGMENT_NAME_PLACEHOLDER;
    this._timestamp = TIMESTAMP_PLACEHOLDER;
    this._kafkaOffset = KAFKA_OFFSET_PLACEHOLDER;
    this._version = version;
  }

  public byte[] getKey() {
    Preconditions.checkState(!isVersionMessage(), "Cannot get key from a version message");
    return _key;
  }

  public String getSegmentName() {
    Preconditions.checkState(!isVersionMessage(), "Cannot get segment name from a version message");
    return _segmentName;
  }

  public long getTimestamp() {
    Preconditions.checkState(!isVersionMessage(), "Cannot get timestamp from a version message");
    return _timestamp;
  }

  public long getKafkaOffset() {
    Preconditions.checkState(!isVersionMessage(), "Cannot get kafka offset from a version message");
    return _kafkaOffset;
  }

  public long getVersion() {
    Preconditions.checkState(isVersionMessage(), "Cannot get version from a regular ingestion upsert event");
    return _version;
  }

  public boolean isVersionMessage() {
    return _version > VERSION_PLACEHOLDER;
  }

  @Override
  public String toString() {
    return "key: " + new String(_key, StandardCharsets.UTF_8)
        + " segment: " + _segmentName
        + " timestamp: " + _timestamp
        + " kafkaOffset: " + _kafkaOffset
        + " version: " + _version;
  }

  public KeyCoordinatorMessageContext getContext() {
    return new KeyCoordinatorMessageContext(_segmentName, _timestamp, _kafkaOffset);
  }

  /**
   * get table name without type info
   */
  public String getPinotTableName() {
    return new LLCSegmentName(_segmentName).getTableName();
  }
}

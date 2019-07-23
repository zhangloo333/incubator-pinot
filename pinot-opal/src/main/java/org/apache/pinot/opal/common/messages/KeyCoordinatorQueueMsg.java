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

import org.apache.pinot.common.utils.LLCSegmentName;

import java.io.Serializable;
import java.util.Map;

/**
 * class used for the message pass from pint ingestor and the key coordinator
 * it contains the following field: table name, primary key and other metadata (in the _context field)
 */
public class KeyCoordinatorQueueMsg implements Serializable {
  private final byte[] _key;
  private final String _segmentName;
  private final long _kafkaOffset;
  private final long _timestamp;

  public KeyCoordinatorQueueMsg(byte[] key, String segmentName, long timestamp, long kafkaOffset) {
    this._key = key;
    this._segmentName = segmentName;
    this._timestamp = timestamp;
    this._kafkaOffset = kafkaOffset;
  }

  public byte[] getKey() {
    return _key;
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

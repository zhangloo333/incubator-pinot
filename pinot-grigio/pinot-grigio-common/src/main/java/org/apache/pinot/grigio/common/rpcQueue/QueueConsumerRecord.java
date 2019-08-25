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
package org.apache.pinot.grigio.common.rpcQueue;

/**
 * data wrapper class for kafka ConsumerRecord data
 * created to make grigio-common kafka library neutral (prevent kafka dependency)
 */
public class QueueConsumerRecord<K, V> {

  private final String _topic;
  private final int _partition;
  private final long _offset;
  private final K _key;
  private final V _record;

  public QueueConsumerRecord(String topic, int partition, long offset, K key, V record) {
    this._topic = topic;
    this._partition = partition;
    this._offset = offset;
    this._key = key;
    this._record = record;
  }

  public String getTopic() {
    return _topic;
  }

  public int getPartition() {
    return _partition;
  }

  public long getOffset() {
    return _offset;
  }

  public K getKey() {
    return _key;
  }

  public V getRecord() {
    return _record;
  }
}

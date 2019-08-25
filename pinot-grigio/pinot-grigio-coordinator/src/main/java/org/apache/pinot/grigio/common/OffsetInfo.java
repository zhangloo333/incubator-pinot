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
package org.apache.pinot.grigio.common;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.pinot.grigio.common.rpcQueue.QueueConsumerRecord;

import java.util.HashMap;
import java.util.Map;

public class OffsetInfo {
  private Map<TopicPartition, Long> _offsetMap;

  public OffsetInfo() {
    _offsetMap = new HashMap<>();
  }

  public void updateOffsetIfNecessary(QueueConsumerRecord record) {
    TopicPartition tp = getTopicPartitionFromRecord(record);
    long offset = record.getOffset() + 1;
    if (!_offsetMap.containsKey(tp) || _offsetMap.get(tp) < offset) {
      _offsetMap.put(tp, offset);
    }
  }

  public Map<TopicPartition, OffsetAndMetadata> getOffsetMap() {
    Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>();
    for (Map.Entry<TopicPartition, Long> entry: _offsetMap.entrySet()) {
      offsetAndMetadataMap.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
    }
    return offsetAndMetadataMap;
  }

  private TopicPartition getTopicPartitionFromRecord(QueueConsumerRecord record) {
    return new TopicPartition(record.getTopic(), record.getPartition());
  }
}

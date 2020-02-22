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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;


/**
 * Fixed partition count partitioner that partition with the bytes of the primary key
 */
public class FixedPartitionCountBytesPartitioner extends FixedPartitionCountPartitioner {

  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    if (keyBytes == null) {
      throw new IllegalArgumentException("Cannot partition without a key");
    }
    int numPartitions = cluster.partitionCountForTopic(topic);
    int partitionCount = getPartitionCount();
    if (partitionCount > numPartitions) {
      throw new IllegalArgumentException(String
          .format("Cannot partition to %d partitions for records in topic %s, which has only %d partitions.",
              partitionCount, topic, numPartitions));
    }
    return Utils.toPositive(Utils.murmur2(keyBytes)) % partitionCount;
  }
}

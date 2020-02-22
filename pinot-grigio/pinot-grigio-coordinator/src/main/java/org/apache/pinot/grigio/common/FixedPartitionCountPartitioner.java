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

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;


/**
 * Kafka partitioner that partition records to a fixed number of partitions.
 * i.e., partition results will not change even when more partitions are added to the Kafka topic
 */
public abstract class FixedPartitionCountPartitioner implements Partitioner {

  private static final String PARTITION_COUNT = "partition.count";
  private int partitionCount;

  int getPartitionCount() {
    return partitionCount;
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> configs) {
    partitionCount = Integer.parseInt((String)configs.get(PARTITION_COUNT));
    Preconditions.checkState(partitionCount > 0, "Partition count must be greater than 0");
  }
}

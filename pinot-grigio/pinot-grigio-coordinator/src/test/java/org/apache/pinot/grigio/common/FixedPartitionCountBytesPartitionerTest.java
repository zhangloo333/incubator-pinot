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

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.Cluster;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class FixedPartitionCountBytesPartitionerTest {

  private Map<String, String> configs;

  @BeforeClass
  public void setUp() {
    configs = new HashMap<>();
    configs.put("partition.count", "4");
  }

  @Test
  public void testPartition() {
    FixedPartitionCountBytesPartitioner partitioner = new FixedPartitionCountBytesPartitioner();
    partitioner.configure(configs);

    String topic1 = "test-topic1";
    String topic2 = "test-topic2";
    Cluster cluster = mock(Cluster.class);
    when(cluster.partitionCountForTopic(topic1)).thenReturn(4);
    when(cluster.partitionCountForTopic(topic2)).thenReturn(8);

    String key = "test-key";
    byte[] keyBytes = key.getBytes();

    int partitionResult1 = partitioner.partition(topic1, key, keyBytes, null, null, cluster);
    int partitionResult2 = partitioner.partition(topic2, key, keyBytes, null, null, cluster);
    assertEquals(partitionResult1, partitionResult2);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testPartitionFailed() {
    FixedPartitionCountBytesPartitioner partitioner = new FixedPartitionCountBytesPartitioner();
    partitioner.configure(configs);

    String topic = "test-topic";
    Cluster cluster = mock(Cluster.class);
    when(cluster.partitionCountForTopic(topic)).thenReturn(2);

    partitioner.partition(topic, null, null, null, null, cluster);
  }
}

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
package org.apache.pinot.grigio.keyCoordinator.internal;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.pinot.grigio.common.OffsetInfo;
import org.apache.pinot.grigio.common.messages.KeyCoordinatorQueueMsg;
import org.apache.pinot.grigio.common.rpcQueue.KeyCoordinatorQueueConsumer;
import org.apache.pinot.grigio.common.rpcQueue.QueueConsumerRecord;
import org.apache.pinot.grigio.keyCoordinator.GrigioKeyCoordinatorMetrics;
import org.apache.pinot.grigio.keyCoordinator.starter.KeyCoordinatorConf;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class MessageFetcherTest {

  private MessageFetcher messageFetcher;
  private KeyCoordinatorQueueConsumer mockConsumer;
  private GrigioKeyCoordinatorMetrics mockMetrics;
  private BlockingQueue<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> consumerRecords;
  private int invocationCount;

  @BeforeMethod
  public void init() {
    KeyCoordinatorConf conf = new KeyCoordinatorConf();

    mockConsumer = mock(KeyCoordinatorQueueConsumer.class);
    mockMetrics = mock(GrigioKeyCoordinatorMetrics.class);
    consumerRecords = new ArrayBlockingQueue<>(100);
    invocationCount = 0;
    when(mockConsumer.getRequests(anyLong(), any())).thenAnswer((invocationOnMock) -> {
      invocationCount++;
      List<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> result = new ArrayList<>();
      consumerRecords.drainTo(result);
      return result;
    });
    messageFetcher = new MessageFetcher(conf, mockConsumer, Executors.newFixedThreadPool(1), mockMetrics);
  }

  @Test
  public void testGetMessages() throws InterruptedException {
    List<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> msgList = ImmutableList.of(
        new QueueConsumerRecord<>("topic1", 1, 123, new byte[]{123},
          new KeyCoordinatorQueueMsg(new byte[]{123}, "segment1", 456, 900), 123),
        new QueueConsumerRecord<>("topic1", 2, 156, new byte[]{123},
          new KeyCoordinatorQueueMsg(new byte[]{123}, "segment2", 456, 901),123),
        new QueueConsumerRecord<>("topic1", 1, 140, new byte[]{123},
          new KeyCoordinatorQueueMsg(new byte[]{123}, "segment1", 470, 901), 123));
    msgList.forEach(consumerRecords::offer);
    messageFetcher.start();
    // wait necessary time for ingestion loop to start and processing the data
    // TODO: make the wait smarter so we can ensure the messages are fetched after a certain time
    TimeUnit.MILLISECONDS.sleep(100);
    MessageFetcher.MessageAndOffset<QueueConsumerRecord<byte[], KeyCoordinatorQueueMsg>> result = messageFetcher.getMessages(System.currentTimeMillis() + 500);

    // ensure the invocation is not too much:
    Assert.assertTrue(invocationCount < 10);

    // ensure the offset are handled properly
    Map<TopicPartition, OffsetAndMetadata> offsetMap = result.getOffsetInfo().getOffsetMap();
    Assert.assertEquals(offsetMap.size(), 2);
    Assert.assertEquals(offsetMap.get(new TopicPartition("topic1", 1)).offset(), 141);
    Assert.assertEquals(offsetMap.get(new TopicPartition("topic1", 2)).offset(), 157);

    // ensure the data fetched are correct
    Assert.assertEquals(result.getMessages().size(), 3);
    Assert.assertEquals(result.getMessages().get(0), msgList.get(0));
    Assert.assertEquals(result.getMessages().get(1), msgList.get(1));
    Assert.assertEquals(result.getMessages().get(2), msgList.get(2));

    // test if we fetch message again
    msgList.forEach(consumerRecords::offer);
    TimeUnit.MILLISECONDS.sleep(100);
    result = messageFetcher.getMessages(System.currentTimeMillis() + 100);
    Assert.assertEquals(result.getMessages().size(), 3);

    Assert.assertEquals(result.getMessages().get(0), msgList.get(0));
    Assert.assertEquals(result.getMessages().get(1), msgList.get(1));
    Assert.assertEquals(result.getMessages().get(2), msgList.get(2));
  }

  @Test
  public void testAckOffset() {
    OffsetInfo offsetInfo = new OffsetInfo(
      (Map) ImmutableMap.of(
          new TopicPartition("topic1", 1), 141,
          new TopicPartition("topic1", 2), 150)
    );
    messageFetcher.ackOffset(new MessageFetcher.MessageAndOffset(ImmutableList.of(), offsetInfo));
    verify(mockConsumer, times(1)).ackOffset(offsetInfo);
  }
}
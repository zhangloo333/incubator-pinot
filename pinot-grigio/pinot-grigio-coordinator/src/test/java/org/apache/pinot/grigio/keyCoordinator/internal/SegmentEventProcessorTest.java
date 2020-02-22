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
import org.apache.pinot.grigio.common.keyValueStore.ByteArrayWrapper;
import org.apache.pinot.grigio.common.keyValueStore.KeyValueStoreDB;
import org.apache.pinot.grigio.common.keyValueStore.KeyValueStoreTable;
import org.apache.pinot.grigio.common.messages.KeyCoordinatorMessageContext;
import org.apache.pinot.grigio.common.messages.KeyCoordinatorQueueMsg;
import org.apache.pinot.grigio.common.messages.LogCoordinatorMessage;
import org.apache.pinot.grigio.common.messages.LogEventType;
import org.apache.pinot.grigio.common.rpcQueue.LogCoordinatorQueueProducer;
import org.apache.pinot.grigio.common.rpcQueue.ProduceTask;
import org.apache.pinot.grigio.common.rpcQueue.QueueConsumerRecord;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogEntry;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogStorageProvider;
import org.apache.pinot.grigio.common.storageProvider.retentionManager.UpdateLogRetentionManager;
import org.apache.pinot.grigio.common.storageProvider.retentionManager.UpdateLogTableRetentionManager;
import org.apache.pinot.grigio.common.updateStrategy.MessageResolveStrategy;
import org.apache.pinot.grigio.common.updateStrategy.MessageTimeResolveStrategy;
import org.apache.pinot.grigio.keyCoordinator.GrigioKeyCoordinatorMetrics;
import org.apache.pinot.grigio.keyCoordinator.starter.KeyCoordinatorConf;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.pinot.grigio.keyCoordinator.starter.KeyCoordinatorConf.KC_OUTPUT_TOPIC_PREFIX_KEY;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SegmentEventProcessorTest {

  private KeyValueStoreTable mockTable1;
  private KeyValueStoreTable mockTable2;
  private LogCoordinatorQueueProducer mockProducer;
  private MessageResolveStrategy messageResolveStrategy;
  private KeyValueStoreDB<ByteArrayWrapper, KeyCoordinatorMessageContext> mockDB;
  private UpdateLogStorageProvider mockStorageProvider;
  private UpdateLogRetentionManager mockRetentionManager;
  private VersionMessageManager mockVersionManager;
  private GrigioKeyCoordinatorMetrics mockMetrics;

  private Map<String, List<ProduceTask>> capturedTasks;

  private SegmentEventProcessor processor;

  @BeforeMethod
  public void init() throws IOException {
    KeyCoordinatorConf conf = new KeyCoordinatorConf();
    conf.addProperty(KC_OUTPUT_TOPIC_PREFIX_KEY, "prefix_");

    // all mocks
    mockProducer = mock(LogCoordinatorQueueProducer.class);
    messageResolveStrategy = new MessageTimeResolveStrategy();
    mockDB = mock(KeyValueStoreDB.class);
    mockStorageProvider = mock(UpdateLogStorageProvider.class);
    mockRetentionManager = mock(UpdateLogRetentionManager.class);
    mockVersionManager = mock(VersionMessageManager.class);
    mockMetrics = mock(GrigioKeyCoordinatorMetrics.class);

    // inner mock for retentionManager
    UpdateLogTableRetentionManager mockTableRetentionManager = mock(UpdateLogTableRetentionManager.class);
    when(mockRetentionManager.getRetentionManagerForTable(anyString())).thenReturn(mockTableRetentionManager);
    when(mockTableRetentionManager.shouldIngestForSegment(anyString())).thenReturn(true);

    // inner mock for db
    mockTable1 = mock(KeyValueStoreTable.class);
    mockTable2 = mock(KeyValueStoreTable.class);
    when(mockDB.getTable("table1")).thenReturn(mockTable1);
    when(mockDB.getTable("table2")).thenReturn(mockTable2);
    when(mockTable1.multiGet(anyList())).thenReturn(ImmutableMap.of());
    when(mockTable2.multiGet(anyList())).thenReturn(
        ImmutableMap.of(new ByteArrayWrapper(new byte[]{13}),
            new KeyCoordinatorMessageContext("table2__0__9__20191027T2041Z", 90, 300)));

    // mock version
    Map<Integer, Long> versionMapping = new HashMap<>();
    when(mockVersionManager.getVersionConsumed(anyInt())).thenAnswer(invocationOnMock -> {
      int partition = invocationOnMock.getArgument(0);
      return versionMapping.getOrDefault(partition, 9l);
    });
    doAnswer(invocationOnMock -> {
      int partition = invocationOnMock.getArgument(0);
      long version = invocationOnMock.getArgument(1);
      versionMapping.put(partition, version);
      return null;
    }).when(mockVersionManager).maybeUpdateVersionConsumed(anyInt(), anyLong());


    capturedTasks = new HashMap<>();
    // mock producer
    doAnswer(invocationOnMock -> {
      List<ProduceTask> produceTasks = invocationOnMock.getArgument(0);
      for (ProduceTask produceTask : produceTasks) {
        produceTask.markComplete(null, null);
        capturedTasks.computeIfAbsent(produceTask.getTopic(), t -> new ArrayList<>()).add(produceTask);
      }
      return null;
    }).when(mockProducer).batchProduce(anyList());

    processor = new SegmentEventProcessor(conf, mockProducer, messageResolveStrategy, mockDB, mockStorageProvider,
        mockRetentionManager, mockVersionManager, mockMetrics);
  }

  @Test
  public void testProcessMessages() throws IOException {
    ImmutableList msgList = ImmutableList.copyOf(new QueueConsumerRecord[]{
        // side effect: generate 1 insert for key 13
        new QueueConsumerRecord<>("topic", 1, 45, new byte[]{13},
            new KeyCoordinatorQueueMsg(new byte[]{13}, "table1__0__10__20191027T2041Z", 100, 500), 123),
        // side effect: generate 1 insert & 1 delete for key 13
        new QueueConsumerRecord<>("topic", 1, 45, new byte[]{13},
            new KeyCoordinatorQueueMsg(new byte[]{13}, "table1__0__10__20191027T2041Z", 120, 600), 123),
        // side effect: generate 1 insert for key 13 at table 2, verify different tables works
        // also 1 delete message on existing message in kv store
        new QueueConsumerRecord<>("topic", 1, 45, new byte[]{13},
            new KeyCoordinatorQueueMsg(new byte[]{13}, "table2__0__10__20191027T2041Z", 120, 600), 123),
        // side effect: version message should update versions
        new QueueConsumerRecord<>("topic", 1, 45, new byte[]{12},
            new KeyCoordinatorQueueMsg(10), 123),
        // side effect: generate 1 insert for new key 14
        new QueueConsumerRecord<>("topic", 1, 45, new byte[]{14},
            new KeyCoordinatorQueueMsg(new byte[]{14}, "table1__0__10__20191027T2041Z", 120, 700), 123),
        // side effect: generate no insert/delete message due to older timestamp
        new QueueConsumerRecord<>("topic", 1, 45, new byte[]{13},
            new KeyCoordinatorQueueMsg(new byte[]{13}, "table1__0__10__20191027T2041Z", 90, 800), 123),
        // side effect: generate no update message as we reprocess same message
        new QueueConsumerRecord<>("topic", 1, 45, new byte[]{13},
            new KeyCoordinatorQueueMsg(new byte[]{13}, "table2__0__10__20191027T2041Z", 120, 600), 123),
        // side effect: generate 1 insert & 1 delete for key 13 at table 2
        new QueueConsumerRecord<>("topic", 1, 45, new byte[]{45},
            new KeyCoordinatorQueueMsg(new byte[]{13}, "table2__0__11__20191027T2041Z", 140, 800), 123),
        }
    );
    processor.start();
    processor.processMessages(msgList);

    // verify kafka output
    List<ProduceTask> table1Tasks = capturedTasks.get("prefix_table1");
    Assert.assertEquals(table1Tasks.size(), 4);
    Assert.assertEquals(table1Tasks.get(0), new ProduceTask<>("prefix_table1", 1,
        new LogCoordinatorMessage("table1__0__10__20191027T2041Z", 500, 9, LogEventType.INSERT)));
    Assert.assertEquals(table1Tasks.get(1), new ProduceTask<>("prefix_table1", 1,
        new LogCoordinatorMessage("table1__0__10__20191027T2041Z", 500, 9, LogEventType.DELETE)));
    Assert.assertEquals(table1Tasks.get(2), new ProduceTask<>("prefix_table1", 1,
        new LogCoordinatorMessage("table1__0__10__20191027T2041Z", 600, 9, LogEventType.INSERT)));
    Assert.assertEquals(table1Tasks.get(3), new ProduceTask<>("prefix_table1", 1,
        new LogCoordinatorMessage("table1__0__10__20191027T2041Z", 700, 10, LogEventType.INSERT)));

    List<ProduceTask> table2Tasks = capturedTasks.get("prefix_table2");
    Assert.assertEquals(table2Tasks.size(), 4);
    Assert.assertEquals(table2Tasks.get(0), new ProduceTask<>("prefix_table2", 1,
        new LogCoordinatorMessage("table2__0__9__20191027T2041Z", 300, 9, LogEventType.DELETE)));
    Assert.assertEquals(table2Tasks.get(1), new ProduceTask<>("prefix_table2", 1,
        new LogCoordinatorMessage("table2__0__10__20191027T2041Z", 600, 9, LogEventType.INSERT)));
    Assert.assertEquals(table2Tasks.get(2), new ProduceTask<>("prefix_table2", 1,
        new LogCoordinatorMessage("table2__0__10__20191027T2041Z", 600, 10, LogEventType.DELETE)));
    Assert.assertEquals(table2Tasks.get(3), new ProduceTask<>("prefix_table2", 1,
        new LogCoordinatorMessage("table2__0__11__20191027T2041Z", 800, 10, LogEventType.INSERT)));

    // verify kv storage
    verify(mockTable1).multiPut(ImmutableMap.of(
        new ByteArrayWrapper(new byte[]{13}),
        new KeyCoordinatorMessageContext("table1__0__10__20191027T2041Z", 120, 600),
        new ByteArrayWrapper(new byte[]{14}),
        new KeyCoordinatorMessageContext("table1__0__10__20191027T2041Z", 120, 700)
    ));
    verify(mockTable2).multiPut(ImmutableMap.of(
        new ByteArrayWrapper(new byte[]{13}),
        new KeyCoordinatorMessageContext("table2__0__11__20191027T2041Z", 140, 800)
    ));

    // verify local log storage
    verify(mockStorageProvider).addDataToFile("table1_REALTIME", "table1__0__10__20191027T2041Z",
        ImmutableList.of(
            new UpdateLogEntry(500, 9, LogEventType.INSERT, 1),
            new UpdateLogEntry(500, 9, LogEventType.DELETE, 1),
            new UpdateLogEntry(600, 9, LogEventType.INSERT, 1),
            new UpdateLogEntry(700, 10, LogEventType.INSERT, 1)
        )
    );
    verify(mockStorageProvider).addDataToFile("table2_REALTIME", "table2__0__9__20191027T2041Z",
        ImmutableList.of(
            new UpdateLogEntry(300, 9, LogEventType.DELETE, 1)
        )
    );
    verify(mockStorageProvider).addDataToFile("table2_REALTIME", "table2__0__10__20191027T2041Z",
        ImmutableList.of(
            new UpdateLogEntry(600, 9, LogEventType.INSERT, 1),
            new UpdateLogEntry(600, 10, LogEventType.DELETE, 1)
        )
    );
    verify(mockStorageProvider).addDataToFile("table2_REALTIME", "table2__0__11__20191027T2041Z",
        ImmutableList.of(
            new UpdateLogEntry(800, 10, LogEventType.INSERT, 1)
        )
    );
  }
}
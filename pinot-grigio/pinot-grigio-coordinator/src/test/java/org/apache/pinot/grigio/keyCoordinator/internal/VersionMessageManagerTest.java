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

import com.google.common.collect.ImmutableMap;
import org.apache.pinot.grigio.common.rpcQueue.VersionMsgQueueProducer;
import org.apache.pinot.grigio.keyCoordinator.GrigioKeyCoordinatorMetrics;
import org.apache.pinot.grigio.keyCoordinator.helix.KeyCoordinatorLeadershipManager;
import org.apache.pinot.grigio.keyCoordinator.helix.KeyCoordinatorVersionManager;
import org.apache.pinot.grigio.keyCoordinator.starter.KeyCoordinatorConf;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Timer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


public class VersionMessageManagerTest {

  private VersionMsgQueueProducer mockProducer;
  private KeyCoordinatorVersionManager mockVersionManager;
  private KeyCoordinatorLeadershipManager mockLeadershipManager;
  private GrigioKeyCoordinatorMetrics mockMetrics;

  private VersionMessageManager versionMessageManager;

  @BeforeMethod
  public void init() {
    mockProducer = mock(VersionMsgQueueProducer.class);
    mockVersionManager = mock(KeyCoordinatorVersionManager.class);
    mockLeadershipManager = mock(KeyCoordinatorLeadershipManager.class);
    mockMetrics = mock(GrigioKeyCoordinatorMetrics.class);

    KeyCoordinatorConf conf = new KeyCoordinatorConf();
    Timer timer = new Timer();

    versionMessageManager = new VersionMessageManager(conf, mockProducer, timer, mockVersionManager,
        mockLeadershipManager, mockMetrics);
  }

  @Test
  public void testSetVersionConsumedToPropertyStore() {

    versionMessageManager.setVersionConsumedToPropertyStore();
    verify(mockVersionManager).setVersionConsumedToPropertyStore(ImmutableMap.of());

    versionMessageManager.maybeUpdateVersionConsumed(1, 2l);
    versionMessageManager.maybeUpdateVersionConsumed(2, 3l);
    versionMessageManager.maybeUpdateVersionConsumed(2, 4l);
    versionMessageManager.setVersionConsumedToPropertyStore();

    verify(mockVersionManager).setVersionConsumedToPropertyStore(ImmutableMap.of(1, 2l, 2, 4l));
  }

  @Test
  public void testGetAndUpdateVersionConsumed() {
    Assert.assertEquals(versionMessageManager.getVersionConsumed(0), 0);

    versionMessageManager.maybeUpdateVersionConsumed(1, 2);
    Assert.assertEquals(versionMessageManager.getVersionConsumed(0), 0);
    Assert.assertEquals(versionMessageManager.getVersionConsumed(1), 2);

    versionMessageManager.maybeUpdateVersionConsumed(0, 1);
    Assert.assertEquals(versionMessageManager.getVersionConsumed(0), 1);
    Assert.assertEquals(versionMessageManager.getVersionConsumed(1), 2);

    versionMessageManager.maybeUpdateVersionConsumed(0, 4);
    Assert.assertEquals(versionMessageManager.getVersionConsumed(0), 4);
    Assert.assertEquals(versionMessageManager.getVersionConsumed(1), 2);

    versionMessageManager.maybeUpdateVersionConsumed(0, 3);
    Assert.assertEquals(versionMessageManager.getVersionConsumed(0), 4);
    Assert.assertEquals(versionMessageManager.getVersionConsumed(1), 2);
  }
}
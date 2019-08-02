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
package org.apache.pinot.opal.common.rpcQueue;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;

public class ProduceTaskTest {

  private ProduceTask<String, String> task;

  @BeforeMethod
  public void setup() {
    this.task = new ProduceTask<>("topic", "key", "value");
  }

  @Test
  public void testGets() {
    Assert.assertEquals(task.getTopic(), "topic");
    Assert.assertEquals(task.getKey(), "key");
    Assert.assertEquals(task.getValue(), "value");
  }

  @Test
  public void testSetCountDownLatch() {
    CountDownLatch latch = new CountDownLatch(2);
    task.setCountDownLatch(latch);
    task.markComplete(new Object(), null);
    Assert.assertEquals(latch.getCount(), 1);
    task = new ProduceTask<>("key", "value");
    task.setCountDownLatch(latch);
    task.markComplete(null, new Exception());
    Assert.assertEquals(latch.getCount(), 0);
  }

  @Test
  public void testMarkComplete() {
    task.markComplete(new Object(), null);
    Assert.assertTrue(task.isSucceed());
  }

  @Test
  public void testMarkException() {
    task.markComplete(new Object(), new Exception());
    Assert.assertTrue(!task.isSucceed());
  }
}
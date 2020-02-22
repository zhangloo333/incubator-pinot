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

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

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
  public void testSetCallback() {

    final AtomicInteger successCount = new AtomicInteger(0);
    final AtomicInteger failureCount = new AtomicInteger(0);
    ProduceTask.Callback callback = new ProduceTask.Callback() {
      @Override
      public void onSuccess() {
        successCount.incrementAndGet();
      }

      @Override
      public void onFailure(Exception ex) {
        failureCount.incrementAndGet();
      }
    };
    task.setCallback(callback);
    task.markComplete(new Object(), null);
    Assert.assertEquals(successCount.get(), 1);
    Assert.assertEquals(failureCount.get(), 0);
    task = new ProduceTask<>("key", "value");
    task.setCallback(callback);
    task.markComplete(null, new Exception());
    Assert.assertEquals(successCount.get(), 1);
    Assert.assertEquals(failureCount.get(), 1);
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
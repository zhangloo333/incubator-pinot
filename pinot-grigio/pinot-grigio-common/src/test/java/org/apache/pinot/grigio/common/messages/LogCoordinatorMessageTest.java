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
package org.apache.pinot.grigio.common.messages;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class LogCoordinatorMessageTest {

  private LogCoordinatorMessage message;

  @BeforeClass
  public void setup() {
    message = new LogCoordinatorMessage("name", 123, 1, LogEventType.INSERT);
  }

  @Test
  public void testGets() {
    Assert.assertEquals(message.getSegmentName(), "name");
    Assert.assertEquals(message.getKafkaOffset(), 123);
    Assert.assertEquals(message.getValue(), 1);
    Assert.assertEquals(message.getUpdateEventType(), LogEventType.INSERT);

  }

}
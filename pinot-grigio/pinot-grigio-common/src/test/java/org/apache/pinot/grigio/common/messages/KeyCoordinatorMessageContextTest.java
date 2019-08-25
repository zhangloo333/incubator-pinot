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

public class KeyCoordinatorMessageContextTest {

  private KeyCoordinatorMessageContext context;

  @BeforeClass
  public void setup() {
    context = new KeyCoordinatorMessageContext("name", 123, 100);
  }

  @Test
  public void testGetMembers() {
    Assert.assertEquals(context.getSegmentName(), "name");
    Assert.assertEquals(context.getTimestamp(), 123);
    Assert.assertEquals(context.getKafkaOffset(), 100);
  }

  @Test
  public void testSerialDeserial() {
    Assert.assertEquals(KeyCoordinatorMessageContext.fromBytes(context.toBytes()).get(), context);
  }

  @Test
  public void testEquals() {
    Assert.assertEquals(context, new KeyCoordinatorMessageContext("name", 123, 100));
    Assert.assertNotEquals(context, new KeyCoordinatorMessageContext("name1", 123, 100));
    Assert.assertNotEquals(context, new KeyCoordinatorMessageContext("name", 12, 100));
    Assert.assertNotEquals(context, new KeyCoordinatorMessageContext("name", 123, 0));
  }

}
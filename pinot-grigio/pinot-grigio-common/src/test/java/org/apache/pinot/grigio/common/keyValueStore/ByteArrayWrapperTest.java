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
package org.apache.pinot.grigio.common.keyValueStore;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class ByteArrayWrapperTest {

  private byte[] data = new byte[]{0, 1};
  private ByteArrayWrapper wrapper = new ByteArrayWrapper(data);

  @BeforeTest
  public void setup() {
  }

  @Test
  public void testGetData() {
    Assert.assertEquals(wrapper.getData(), data);
  }

  @Test
  public void testEquals1() {
    Assert.assertEquals(wrapper, new ByteArrayWrapper(new byte[]{0, 1}));
    Assert.assertNotEquals(wrapper, new ByteArrayWrapper(new byte[]{}));
    Assert.assertNotEquals(wrapper, new ByteArrayWrapper(new byte[]{1, 1}));
  }
}
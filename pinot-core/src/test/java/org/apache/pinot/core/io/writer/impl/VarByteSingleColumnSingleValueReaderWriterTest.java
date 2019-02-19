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
package org.apache.pinot.core.io.writer.impl;

import org.apache.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.core.io.readerwriter.impl.VarByteSingleColumnSingleValueReaderWriter;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VarByteSingleColumnSingleValueReaderWriterTest {

  @Test
  public void testSimple() {
    VarByteSingleColumnSingleValueReaderWriter readerWriter;
    PinotDataBufferMemoryManager mem = new DirectMemoryManager("test");
    readerWriter = new VarByteSingleColumnSingleValueReaderWriter(100, -1, mem, "test");

    for (int i = 0; i < 10000; i++) {
      String data = "TEST-" + i;
      readerWriter.setBytes(i, data.getBytes());
    }
    boolean passed = true;
    for (int i = 0; i < 10000; i++) {
      byte[] data = readerWriter.getBytes(i);
      if (!new String(data).equals("TEST-" + i)) {
        passed = false;
        break;
      }
    }
    Assert.assertTrue(passed);
  }
}

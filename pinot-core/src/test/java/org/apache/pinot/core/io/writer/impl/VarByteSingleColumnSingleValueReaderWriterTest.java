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

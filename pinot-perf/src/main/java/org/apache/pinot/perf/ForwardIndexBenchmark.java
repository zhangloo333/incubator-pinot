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
package org.apache.pinot.perf;

import com.google.common.base.Stopwatch;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import me.lemire.integercompression.BitPacking;
import org.apache.commons.math.util.MathUtils;
import org.apache.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader;
import org.apache.pinot.core.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.core.io.util.FixedByteValueReaderWriter;
import org.apache.pinot.core.io.util.PinotDataBitSet;
import org.apache.pinot.core.io.writer.impl.v1.FixedBitSingleValueWriter;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;

public class ForwardIndexBenchmark {

  static int ROWS = 36_000_000;
  static int MAX_VALUE = 40000;
  static int NUM_BITS = PinotDataBitSet.getNumBitsPerValue(MAX_VALUE);
  static File rawFile = new File("/Users/steotia/fwd-index.test");
  static File pinotOutFile = new File(rawFile.getAbsolutePath() + ".pinot.fwd");
  static File bitPackedFile = new File(rawFile.getAbsolutePath() + ".fast.fwd");

  static {
    rawFile.delete();
    pinotOutFile.delete();
    bitPackedFile.delete();
  }

  static void generateRawFile()
      throws IOException {

    rawFile.delete();
    BufferedWriter bw = new BufferedWriter(new FileWriter(rawFile));
    Random r = new Random();
    for (int i = 0; i < ROWS; i++) {
      bw.write("" + r.nextInt(40000));
      bw.write("\n");
    }
    bw.close();
  }

  static void generatePinotFwdIndex()
      throws Exception {
    BufferedReader bfr = new BufferedReader(new FileReader(rawFile));
    FixedBitSingleValueWriter fixedBitSingleValueWriter = new FixedBitSingleValueWriter(pinotOutFile, ROWS, NUM_BITS);
    String line;
    int rowId = 0;
    while ((line = bfr.readLine()) != null) {
      fixedBitSingleValueWriter.setInt(rowId++, Integer.parseInt(line));
    }
    fixedBitSingleValueWriter.close();
    System.out.println("pinotOutFile.length = " + pinotOutFile.length());
  }

  static void generatePFORIndex()
      throws Exception {

    BufferedReader bfr = new BufferedReader(new FileReader(rawFile));

    String line;
    int rowId = 0;
    int[] data = new int[ROWS];
    while ((line = bfr.readLine()) != null) {
      data[rowId++] = Integer.parseInt(line);
    }
    int bits = MathUtils.lcm(32, NUM_BITS);
    int inputSize = 32;
    int outputSize = 32 * NUM_BITS / 32;
    int[] raw = new int[inputSize];
    int[] bitPacked = new int[outputSize];

    int totalNum = (NUM_BITS * ROWS + 31) / Integer.SIZE;

    PinotDataBuffer pinotDataBuffer = PinotDataBuffer
        .mapFile(bitPackedFile, false, 0, (long) totalNum * Integer.BYTES, ByteOrder.BIG_ENDIAN, "bitpacking");

    FixedByteValueReaderWriter readerWriter = new FixedByteValueReaderWriter(pinotDataBuffer);
    int counter = 0;
    for (int i = 0; i < data.length; i++) {
      raw[counter] = data[i];
      if (counter == raw.length - 1 || i == data.length - 1) {
        BitPacking.fastpack(raw, 0, bitPacked, 0, NUM_BITS);
        for (int j = 0; j < outputSize; j++) {
          readerWriter.writeInt(i / 32 + j, bitPacked[j]);
        }
        Arrays.fill(bitPacked, 0);
        counter = 0;
      } else {
        counter = counter + 1;
      }
    }
    readerWriter.close();
    System.out.println("bitPackedFile.length = " + bitPackedFile.length());
  }

  static void readRawFile()
      throws IOException {
    BufferedReader bfr = new BufferedReader(new FileReader(rawFile));
    String line;
    int rowId = 0;
    int[] values = new int[ROWS];
    while ((line = bfr.readLine()) != null) {
      values[rowId++] = Integer.parseInt(line);
    }
//    System.out.println("raw = " + Arrays.toString(values));
  }

  static void readPinotFwdIndex()
      throws IOException {
    PinotDataBuffer pinotDataBuffer = PinotDataBuffer.loadBigEndianFile(pinotOutFile);
    FixedBitSingleValueReader reader = new FixedBitSingleValueReader(pinotDataBuffer, ROWS, NUM_BITS);
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    int[] unpacked = new int[32];
    stopwatch.start();
    // sequentially unpack 32 integers at a time
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      reader.readBulk(startIndex, 32, unpacked);
    }
    stopwatch.stop();
    System.out.println("pinot took: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
  }

  static void readBitPackedFwdIndex()
      throws IOException {
    PinotDataBuffer pinotDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(bitPackedFile);
    FileChannel fileChannel = new RandomAccessFile(bitPackedFile, "r").getChannel();
    ByteBuffer buffer =
        fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, bitPackedFile.length()).order(ByteOrder.BIG_ENDIAN);
    int[] compressed = new int[NUM_BITS];
    int[] unpacked = new int[32];
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    // sequentially unpack 32 integers at a time
    for (int i = 0; i < ROWS; i += 32) {
      for (int j = 0; j < compressed.length; j++) {
        compressed[j] = buffer.getInt();
      }
      stopwatch.start();
      BitPacking.fastunpack(compressed, 0, unpacked, 0, NUM_BITS);
      stopwatch.stop();
    }
    //System.out.println("bitPacked = " + Arrays.toString(values) + " took: " + (end - start) + " ms");
    System.out.println("bitPacked took: " + stopwatch.elapsed(TimeUnit.MILLISECONDS)+ " ms");
  }

  public static void main(String[] args)
      throws Exception {
    System.out.println("ROWS = " + ROWS);
    generateRawFile();
    generatePinotFwdIndex();
    generatePFORIndex();
    readRawFile();
    readPinotFwdIndex();
    readBitPackedFwdIndex();
  }
}

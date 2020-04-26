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
import org.apache.pinot.core.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.core.io.util.FixedByteValueReaderWriter;
import org.apache.pinot.core.io.util.PinotDataBitSet;
import org.apache.pinot.core.io.writer.impl.v1.FixedBitSingleValueWriter;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;


@State(Scope.Benchmark)
public class ForwardIndexBenchmark {

  static int ROWS = 36_000_000;
  static int MAX_VALUE = 40000;
  static int NUM_BITS = PinotDataBitSet.getNumBitsPerValue(MAX_VALUE);
  static File rawFile = new File("/Users/kishoreg/fwd-index.test");
  static File pinotOutFile = new File(rawFile.getAbsolutePath() + ".pinot.fwd");
  static File bitPackedFile = new File(rawFile.getAbsolutePath() + ".fast.fwd");
  FixedBitIntReaderWriter reader;
  int[] packed = new int[NUM_BITS];
  int[] unpacked = new int[32];

  ByteBuffer bitPackedBuffer;

  static {
  }

  @Setup
  public void setup()
      throws Exception {
    rawFile.delete();
    pinotOutFile.delete();
    bitPackedFile.delete();
    generateRawFile();
    generatePinotFwdIndex();
    generatePFORIndex();

    //setup pinot forward index reader
    PinotDataBuffer pinotDataBuffer = PinotDataBuffer.loadBigEndianFile(pinotOutFile);
    reader = new FixedBitIntReaderWriter(pinotDataBuffer, ROWS, NUM_BITS);

    FileChannel fileChannel = new RandomAccessFile(bitPackedFile, "r").getChannel();
    bitPackedBuffer =
        fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, bitPackedFile.length()).order(ByteOrder.BIG_ENDIAN);
    bitPackedBuffer.mark();
  }

  public void generateRawFile()
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

  public void generatePinotFwdIndex()
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

  public void generatePFORIndex()
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

  public void readRawFile()
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

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void readPinotFwdIndex()
      throws IOException {
    // sequentially unpack 32 integers at a time
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      reader.readInt(startIndex, 32, unpacked);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void readBitPackedFwdIndex()
      throws IOException {
    // sequentially unpack 32 integers at a time
    bitPackedBuffer.reset();
    for (int i = 0; i < ROWS; i += 32) {
      for (int j = 0; j < packed.length; j++) {
        packed[j] = bitPackedBuffer.getInt();
      }
      BitPacking.fastunpack(packed, 0, unpacked, 0, NUM_BITS);
    }
  }

  public static void main(String[] args)
      throws Exception {
    Options opt =
        new OptionsBuilder().include(ForwardIndexBenchmark.class.getSimpleName()).warmupTime(TimeValue.seconds(2))
            .warmupIterations(3).measurementTime(TimeValue.seconds(5)).measurementIterations(3).forks(1).build();

    new Runner(opt).run();
  }
}

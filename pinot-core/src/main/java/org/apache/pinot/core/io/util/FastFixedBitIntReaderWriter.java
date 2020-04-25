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
package org.apache.pinot.core.io.util;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import me.lemire.integercompression.BitPacking;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;


public final class FastFixedBitIntReaderWriter implements Closeable {
  private final int _numBitsPerValue;
  private final int chunkToPack[];
  private final int packedChunk[];
  private final int unpackedChunk[];
  private int currentIndexInChunk = 0;
  private final PinotDataBuffer _dataBuffer;
  private int _offset = 0;
  private static final int PACK_BOUNDARY = 32;

  // Currently only supports SV columns
  public FastFixedBitIntReaderWriter(PinotDataBuffer dataBuffer, int numValues, int numBitsPerValue) {
    // TODO: this should be rounded off to next multiple of 4
    Preconditions
        .checkState(dataBuffer.size() == (int) (((long) numValues * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE));
    chunkToPack = new int[PACK_BOUNDARY];
    unpackedChunk = new int[numBitsPerValue];
    packedChunk = new int[numBitsPerValue];
    // this should be power of 2 -- 2, 4, 8, 16, 32
    // to ensure that within a 32 bit int value, we can pack 32/numBitsPerValue ints
    // completely without straddling.
    // e.g pack 4 ints into single int by using 8 bits per value
    _numBitsPerValue = numBitsPerValue;
    _dataBuffer = dataBuffer;
  }

  public int readInt(int index) {
    long bitOffset = (long) index * _numBitsPerValue;
    int byteOffset = (int) (bitOffset / Byte.SIZE);
    if (bitOffset % 32 != 0) {
      // read the int containing the packed value at this
      // index and unpack all values packed in that int.
      switch (_numBitsPerValue) {
        case 2:
          fastUnpack2(byteOffset);
        case 4:
          fastUnpack4(byteOffset);
        case 8:
          fastUnpack8(byteOffset);
        case 16:
          fastUnpack16(byteOffset);
      }
    }
    // always keep 32/numBitsPerValue number of integers
    // unpacked by reading a single int. Depending on
    // the value of numBitsPerValue, we could have
    // 2, 4, 8, 16 integers unpacked from a single int
    return unpackedChunk[index % _numBitsPerValue];
  }

  private void fastUnpack16(int byteOffset) {
    int val = _dataBuffer.getInt(byteOffset);
    unpackedChunk[0] = (val & 65535);
    unpackedChunk[1] = (val >>> 16);
  }

  private void fastUnpack8(int byteOffset) {
    int val = _dataBuffer.getInt(byteOffset);
    unpackedChunk[0] = (val & 255);
    unpackedChunk[1] = ((val >>> 8) & 255);
    unpackedChunk[2] = ((val >>> 16) & 255);
    unpackedChunk[3] = (val >>> 24);
  }

  private void fastUnpack4(int byteOffset) {
    int val = _dataBuffer.getInt(byteOffset);
    unpackedChunk[0] = ((val) & 15);
    unpackedChunk[1] = ((val >>> 4) & 15);
    unpackedChunk[2] = ((val >>> 8) & 15);
    unpackedChunk[3] = ((val >>> 12) & 15);
    unpackedChunk[4] = ((val >>> 16) & 15);
    unpackedChunk[5] = ((val >>> 20) & 15);
    unpackedChunk[6] = ((val >>> 24) & 15);
    unpackedChunk[7] = (val >>> 28);
  }

  private void fastUnpack2(int byteOffset) {
    int val = _dataBuffer.getInt(byteOffset);
    unpackedChunk[0] = (val & 3);
    unpackedChunk[1] = ((val >>> 2) & 3);
    unpackedChunk[2] = ((val >>> 4) & 3);
    unpackedChunk[3] = ((val >>> 6) & 3);
    unpackedChunk[4] = ((val >>> 8) & 3);
    unpackedChunk[5] = ((val >>> 10) & 3);
    unpackedChunk[6] = ((val >>> 12) & 3);
    unpackedChunk[7] = ((val >>> 14) & 3);
    unpackedChunk[8] = ((val >>> 16) & 3);
    unpackedChunk[9] = ((val >>> 18) & 3);
    unpackedChunk[10] = ((val >>> 20) & 3);
    unpackedChunk[11] = ((val >>> 22) & 3);
    unpackedChunk[12] = ((val >>> 24) & 3);
    unpackedChunk[13]= ((val >>> 26) & 3);
    unpackedChunk[14] = ((val >>> 28) & 3);
    unpackedChunk[15] = (val >>> 30);
  }

  public void writeInt(int index, int value) {
    if (index > 0 && index % PACK_BOUNDARY == 0) {
      // try to pack 32 integer values at a time
      BitPacking.fastpack(chunkToPack, 0, packedChunk, 0, _numBitsPerValue);
      for (int i = 0; i < _numBitsPerValue; i++) {
        _dataBuffer.putInt(_offset * Integer.BYTES, packedChunk[i]);
        _offset++;
      }
    }
    chunkToPack[currentIndexInChunk] = value;
    currentIndexInChunk = (currentIndexInChunk + 1) % PACK_BOUNDARY;
  }

  @Override
  public void close()
      throws IOException {
  }
}

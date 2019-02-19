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
package org.apache.pinot.core.io.readerwriter.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.io.readerwriter.BaseSingleColumnSingleValueReaderWriter;
import org.apache.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.core.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class VarByteSingleColumnSingleValueReaderWriter extends BaseSingleColumnSingleValueReaderWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(FixedByteSingleColumnSingleValueReaderWriter.class);

  private static int DEFAULT_COLUMN_SIZE_IN_BYTES = 100;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final String _allocationContext;

  private FixedByteSingleValueMultiColumnReaderWriter _headerReaderWriter;
  private List<PinotDataBuffer> _dataBuffers;

  //capacity of the chunk, it can adjust itself based on the avgSize
  private long _chunkCapacityInBytes = 0;
  private final int _numRowsPerChunk;
  private int _avgColumnSizeInBytes;

  //amount of data written to the current buffer
  private int _currentDataSize = 0;
  //number of rows written to the current buffer
  private int _currentBufferRows = 0;
  //PinotDataBuffer where the actual bytes are written to
  private PinotDataBuffer _currentDataBuffer;
  //index pointing to the element in _dataBuffers
  private int _currentDataBufferId;

  /**
   * @param numRowsPerChunk Number of rows to pack in one chunk before a new chunk is created.
   * @param avgColumnSizeInBytes Max Size of column value in bytes. Set this to -1 if its unknown
   * @param memoryManager Memory manager to be used for allocating memory.
   * @param allocationContext Allocation allocationContext.
   */
  public VarByteSingleColumnSingleValueReaderWriter(int numRowsPerChunk, int avgColumnSizeInBytes,
      PinotDataBufferMemoryManager memoryManager, String allocationContext) {
    _avgColumnSizeInBytes = avgColumnSizeInBytes;
    if (avgColumnSizeInBytes < 0) {
      _avgColumnSizeInBytes = DEFAULT_COLUMN_SIZE_IN_BYTES;
    }
    _numRowsPerChunk = numRowsPerChunk;
    _memoryManager = memoryManager;
    _allocationContext = allocationContext;
    _dataBuffers = new ArrayList<>();
    //bufferId, Offset, length for each row
    //we can eliminate the length as an optimization later
    _headerReaderWriter =
        new FixedByteSingleValueMultiColumnReaderWriter(_numRowsPerChunk, new int[]{4, 4, 4}, _memoryManager,
            _allocationContext);
  }

  @Override
  public void close()
      throws IOException {
    for (PinotDataBuffer buffer : _dataBuffers) {
      buffer.close();
    }
  }

  @Override
  public void setString(int row, String value) {
    setBytes(row, StringUtil.encodeUtf8(value));
  }

  @Override
  public String getString(int row) {
    return StringUtil.decodeUtf8(getBytes(row));
  }

  @Override
  public void setBytes(int row, byte[] buf) {

    if (_currentDataSize + buf.length >= _chunkCapacityInBytes) {
      addDataBuffer();
      System.out.println("Added data buffer row:" + row + " numDataBuffers:" + _dataBuffers.size());
    }
    try {
      _headerReaderWriter.setInt(row, 0, _currentDataBufferId);
      _headerReaderWriter.setInt(row, 1, _currentDataSize);
      _headerReaderWriter.setInt(row, 2, buf.length);
      _currentDataBuffer.readFrom(_currentDataSize, buf);
    } catch (Exception e) {
      e.printStackTrace();
    }
    _currentDataSize = _currentDataSize + buf.length;
  }

  @Override
  public byte[] getBytes(int row) {
    int dataBufferId = _headerReaderWriter.getInt(row, 0);
    int startOffset = _headerReaderWriter.getInt(row, 1);
    int length = _headerReaderWriter.getInt(row, 2);
    byte[] buf = new byte[length];
    PinotDataBuffer dataBuffer = _dataBuffers.get(dataBufferId);
    dataBuffer.copyTo(startOffset, buf);
    return buf;
  }

  private void addDataBuffer() {
    //set the avgColumnSize based on the data seen so far.
    if (_currentDataSize > 0 && _currentBufferRows > 0) {
      _avgColumnSizeInBytes = _currentDataSize / _currentBufferRows;
    }
    _chunkCapacityInBytes = _numRowsPerChunk * _avgColumnSizeInBytes;
    LOGGER.info("Allocating bytes for: {}, dataBufferSize: {} ", _allocationContext, _chunkCapacityInBytes);
    PinotDataBuffer dataBuffer = _memoryManager.allocate(_chunkCapacityInBytes, _allocationContext);
    _dataBuffers.add(dataBuffer);
    _currentDataBuffer = dataBuffer;
    //start from 0
    _currentDataBufferId = _dataBuffers.size() - 1;
    _currentDataSize = 0;
  }
}

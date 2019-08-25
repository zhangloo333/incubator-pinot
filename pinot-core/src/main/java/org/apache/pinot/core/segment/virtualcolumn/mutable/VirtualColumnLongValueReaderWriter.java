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
package org.apache.pinot.core.segment.virtualcolumn.mutable;

import org.apache.pinot.core.io.reader.impl.ChunkReaderContext;
import org.apache.pinot.core.segment.virtualcolumn.VirtualColumnContext;
import org.apache.pinot.grigio.common.messages.LogEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public abstract class VirtualColumnLongValueReaderWriter extends BaseVirtualColumnSingleValueReaderWriter<ChunkReaderContext> {

  private static final Logger LOGGER = LoggerFactory.getLogger(VirtualColumnLongValueReaderWriter.class);
  private static final long DEFAULT_VALUE = Long.MIN_VALUE;

  private final VirtualColumnContext _context;
  private int _totalDocSize;
  private int _currentMaxDocId;
  private final long[] _values;
  private final long DEFAULT_NEW_VALUE = -1;

  public VirtualColumnLongValueReaderWriter(VirtualColumnContext context) {
    _context = context;
    _totalDocSize = context.getTotalDocCount();
    _values = new long[_totalDocSize];
    if (!_context.isMutableSegment()) {
      Arrays.fill(_values, -1);
    }
  }

  @Override
  public synchronized void addNewRecord(int docId) {
    if (docId >= _totalDocSize) {
      throw new RuntimeException(String.format("failed to add docId %s larger than allocated size %s", docId, _totalDocSize));
    }
    _currentMaxDocId = docId;
    _values[docId] = DEFAULT_NEW_VALUE;
  }

  @Override
  public ChunkReaderContext createContext() {
    return null;
  }

  @Override
  public long getLong(int row) {
    return _values[row];
  }

  @Override
  public long getLong(int rowId, ChunkReaderContext context) {
    return _values[rowId];
  }

  @Override
  public void readValues(int[] rows, int rowStartPos, int rowSize, int[] values, int valuesStartPos) {
    System.arraycopy(rows, rowStartPos, values, valuesStartPos, rowSize);
  }

  @Override
  public void close() throws IOException {
  }

  /**
   * method to update the internal data to value at a given location
   * synchronized to ensure the we will not modify the internal array at the same time from multiple threads
   */
  protected synchronized boolean updateValue(int docId, long value) {
    if (docId >= _totalDocSize) {
      throw new RuntimeException(String.format("new record docId %s is larger than capacity %s", docId, _totalDocSize));
    }
    if (_values[docId] == 0) {
      throw new RuntimeException(String.format("failed to update virtual column: with value %s:%s we are trying to " +
              "update a value that has not been ingested yet, max doc id %s", docId, value, _currentMaxDocId));
    }
    if (_values[docId] == value) {
      return false;
    } else {
      _values[docId] = value;
      return true;
    }
  }

  public abstract boolean update(int docId, long value, LogEventType eventType);
}

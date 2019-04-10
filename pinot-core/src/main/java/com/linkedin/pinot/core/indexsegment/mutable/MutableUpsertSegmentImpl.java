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
package com.linkedin.pinot.core.indexsegment.mutable;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.indexsegment.UpsertSegment;
import com.linkedin.pinot.core.segment.updater.UpsertWaterMarkManager;
import com.linkedin.pinot.opal.common.StorageProvider.UpdateLogEntry;
import com.linkedin.pinot.opal.common.StorageProvider.UpdateLogStorageProvider;
import com.linkedin.pinot.core.segment.virtualcolumn.mutable.VirtualColumnLongValueReaderWriter;
import com.linkedin.pinot.opal.common.messages.LogEventType;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.core.io.reader.DataFileReader;
import org.apache.pinot.core.realtime.impl.RealtimeSegmentConfig;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MutableUpsertSegmentImpl extends MutableSegmentImpl implements UpsertSegment {

  private static final Logger LOGGER = LoggerFactory.getLogger(MutableUpsertSegmentImpl.class);
  private final UpsertWaterMarkManager upsertWaterMarkManager;

  private final String _kafkaOffsetColumnName;

  private final List<VirtualColumnLongValueReaderWriter> _mutableSegmentReaderWriters = new ArrayList<>();
  // to store the update event that arrive before my current record
  // TODO remove this in the later version of design if necessary
  private final Map<Long, UpdateLogEntry> _unmatchedInsertRecords = new ConcurrentHashMap<>();
  private final Map<Long, UpdateLogEntry> _unmatchedDeleteRecords = new ConcurrentHashMap<>();

  public MutableUpsertSegmentImpl(RealtimeSegmentConfig config) {
    super(config);
    _kafkaOffsetColumnName = _schema.getOffsetKey();
    Preconditions.checkState(_schema.isTableForUpsert(), "table should be upsert");
    for (Map.Entry<String, DataFileReader> entry: _virtualColumnIndexReader.entrySet()) {
      String column = entry.getKey();
      DataFileReader reader = entry.getValue();
      if (reader instanceof VirtualColumnLongValueReaderWriter) {
        _logger.info("adding virtual column {} to updatable reader writer list", column);
        _mutableSegmentReaderWriters.add((VirtualColumnLongValueReaderWriter) reader);
      }
    }
    upsertWaterMarkManager = UpsertWaterMarkManager.getInstance();
    LOGGER.info("starting upsert segment with {} reader writer", _mutableSegmentReaderWriters.size());
  }

  @Override
  public void updateVirtualColumn(List<UpdateLogEntry> logEntryList) {
    for (UpdateLogEntry logEntry: logEntryList) {
      boolean updated = false;
      boolean offsetFound = false;
      MutableRoaringBitmap bitmap = getDocIdsForOffset(logEntry.getOffset());
      if (bitmap != null) {
        IntIterator it = bitmap.getIntIterator();
        // we are not really expecting more than 1 record with the same kafka offset unless something is very wrong here
        // TODO add some check over here
        while (it.hasNext()) {
          int docId = it.next();
          offsetFound = true;
          for (VirtualColumnLongValueReaderWriter readerWriter : _mutableSegmentReaderWriters) {
            updated = readerWriter.update(docId, logEntry.getValue(), logEntry.getType()) || updated;
          }
        }
        if (updated) {
          // only update high water mark if it indeed updated something
          upsertWaterMarkManager.processMessage(_tableName, _segmentName, logEntry);
        }
      }
      if (!offsetFound) {
        putEntryToUnmatchMap(logEntry);
      }
    }
  }

  @Override
  protected void postProcessRecords(GenericRow row, int docId) {
    final Long offset = (Long) row.getValue(_kafkaOffsetColumnName);
    for (VirtualColumnLongValueReaderWriter readerWriter: _mutableSegmentReaderWriters) {
      readerWriter.addNewRecord(docId);
    }
    checkForOutstandingRecords(_unmatchedDeleteRecords, offset, docId);
    checkForOutstandingRecords(_unmatchedInsertRecords, offset, docId);
  }

  @Override
  public void initVirtualColumn() throws IOException {
    //TODO add logic for init virtual column
    Preconditions.checkState(_numDocsIndexed == 0, "should init virtual column before ingestion");
    List<UpdateLogEntry> updateLogEntries = UpdateLogStorageProvider.getInstance().getAllMessages(_tableName, _segmentName);
    LOGGER.info("got {} update log entries for current segment {}", updateLogEntries.size(), _segmentName);
    updateLogEntries.forEach(this::putEntryToUnmatchMap);
  }

  private void checkForOutstandingRecords(Map<Long, UpdateLogEntry> unmatchRecordsMap, Long offset, int docId) {
    UpdateLogEntry unmatchedEntry = unmatchRecordsMap.remove(offset);
    if (unmatchedEntry != null) {
      boolean updated = false;
      for (VirtualColumnLongValueReaderWriter readerWriter: _mutableSegmentReaderWriters) {
        updated = readerWriter.update(docId, unmatchedEntry.getValue(), unmatchedEntry.getType()) || updated;
      }
      if (updated) {
        upsertWaterMarkManager.processMessage(_tableName, _segmentName, unmatchedEntry);
      }
    }
  }

  private void putEntryToUnmatchMap(UpdateLogEntry logEntry) {
    final Map<Long, UpdateLogEntry> unmatchedRecords;
    if (logEntry.getType() == LogEventType.INSERT) {
      unmatchedRecords = _unmatchedInsertRecords;
    } else {
      unmatchedRecords = _unmatchedDeleteRecords;
    }
    unmatchedRecords.put(logEntry.getOffset(), logEntry);
  }

  private MutableRoaringBitmap getDocIdsForOffset(long offset) {
    int dictionaryId = _dictionaryMap.get(_kafkaOffsetColumnName).indexOf(offset);
    if (dictionaryId < 0) {
      return null;
    }
    return _invertedIndexMap.get(_kafkaOffsetColumnName).getDocIds(dictionaryId);
  }
}

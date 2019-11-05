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
package org.apache.pinot.core.indexsegment.mutable;

import com.google.common.base.Preconditions;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.indexsegment.UpsertSegment;
import org.apache.pinot.core.io.reader.DataFileReader;
import org.apache.pinot.core.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.core.segment.updater.UpsertWaterMarkManager;
import org.apache.pinot.core.segment.virtualcolumn.mutable.VirtualColumnLongValueReaderWriter;
import org.apache.pinot.grigio.common.messages.LogEventType;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogEntry;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogStorageProvider;
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
  // use map for mapping between kafka offset and docId because we at-most have 1 mutable segment per consumer
  // will use more memory, but we can update this later
  private final Map<Long, Integer> _sourceOffsetToDocId = new ConcurrentHashMap<>();
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
  public synchronized void updateVirtualColumn(List<UpdateLogEntry> logEntryList) {
    for (UpdateLogEntry logEntry: logEntryList) {
      boolean updated = false;
      boolean offsetFound = false;
      Integer docId = _sourceOffsetToDocId.get(logEntry.getOffset());
      if (docId != null) {
        offsetFound = true;
        for (VirtualColumnLongValueReaderWriter readerWriter : _mutableSegmentReaderWriters) {
          updated = readerWriter.update(docId, logEntry.getValue(), logEntry.getType()) || updated;
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
  public String getVirtualColumnInfo(long offset) {
    Integer docId = _sourceOffsetToDocId.get(offset);
    StringBuilder result = new StringBuilder("matched: ");
    if (docId == null) {
      result = new StringBuilder("no doc id found ");
    } else {
      for (VirtualColumnLongValueReaderWriter readerWriter : _mutableSegmentReaderWriters) {
        result.append(readerWriter.getLong(docId)).append("; ");
      }
    }
    if (_unmatchedInsertRecords.containsKey(offset)) {
      result.append(" unmatched insert: ").append(_unmatchedInsertRecords.get(offset).getValue());
    }
    if (_unmatchedDeleteRecords.containsKey(offset)) {
      result.append(" unmatched delete: ").append(_unmatchedDeleteRecords.get(offset).getValue());
    }
    return result.toString();
  }

  @Override
  protected synchronized void postProcessRecords(GenericRow row, int docId) {
    final Long offset = (Long) row.getValue(_kafkaOffsetColumnName);
    for (VirtualColumnLongValueReaderWriter readerWriter: _mutableSegmentReaderWriters) {
      readerWriter.addNewRecord(docId);
    }
    _sourceOffsetToDocId.put(offset, docId);
    checkForOutstandingRecords(_unmatchedDeleteRecords, offset, docId);
    checkForOutstandingRecords(_unmatchedInsertRecords, offset, docId);
  }

  @Override
  public void initVirtualColumn() throws IOException {
    Preconditions.checkState(_numDocsIndexed == 0, "should init virtual column before ingestion");
    List<UpdateLogEntry> updateLogEntries = UpdateLogStorageProvider.getInstance().getAllMessages(_tableName, _segmentName);
    LOGGER.info("got {} update log entries for current segment {}", updateLogEntries.size(), _segmentName);
    // some physical data might have been ingested when we init virtual column, we will go through the normal update
    // flow to ensure we wont miss records
    updateVirtualColumn(updateLogEntries);
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

}

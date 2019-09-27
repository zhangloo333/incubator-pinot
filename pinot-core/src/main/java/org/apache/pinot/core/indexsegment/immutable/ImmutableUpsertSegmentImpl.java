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
package org.apache.pinot.core.indexsegment.immutable;

import com.clearspring.analytics.util.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.core.indexsegment.UpsertSegment;
import org.apache.pinot.core.io.reader.BaseSingleColumnSingleValueReader;
import org.apache.pinot.core.io.reader.DataFileReader;
import org.apache.pinot.core.segment.index.SegmentMetadataImpl;
import org.apache.pinot.core.segment.index.column.ColumnIndexContainer;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.store.SegmentDirectory;
import org.apache.pinot.core.segment.updater.UpsertWaterMarkManager;
import org.apache.pinot.core.segment.virtualcolumn.mutable.VirtualColumnLongValueReaderWriter;
import org.apache.pinot.core.startree.v2.store.StarTreeIndexContainer;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogEntry;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogStorageProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ImmutableUpsertSegmentImpl extends ImmutableSegmentImpl implements UpsertSegment {

  private static final Logger LOGGER = LoggerFactory.getLogger(ImmutableUpsertSegmentImpl.class);

  private final List<VirtualColumnLongValueReaderWriter> _virtualColumnsReaderWriter = new ArrayList<>();

  private final String _tableNameWithType;
  private final String _segmentName;
  private final Schema _schema;
  private final int _totalDoc;
  private final ColumnIndexContainer _offsetColumnIndexContainer;
  private final UpsertWaterMarkManager _upsertWaterMarkManager;
  // right now we use map for this storage. But it will cost 12 bytes per record. It will translate to 12GB for 1B records
  // will look into reducing this cost later
  private final ImmutableMap<Long, Integer> _sourceOffsetToDocId;

  public ImmutableUpsertSegmentImpl(SegmentDirectory segmentDirectory,
                                    SegmentMetadataImpl segmentMetadata,
                                    Map<String, ColumnIndexContainer> columnIndexContainerMap,
                                    @Nullable StarTreeIndexContainer starTreeIndexContainer) {
    super(segmentDirectory, segmentMetadata, columnIndexContainerMap, starTreeIndexContainer);
    Preconditions.checkState(segmentMetadata.getSchema().isTableForUpsert(), "table should be upsert but it is not");
    _tableNameWithType = TableNameBuilder.ensureTableNameWithType(segmentMetadata.getTableName(),
        CommonConstants.Helix.TableType.REALTIME);
    _segmentName = segmentMetadata.getName();
    _schema = segmentMetadata.getSchema();
    _totalDoc = segmentMetadata.getTotalDocs();
    _offsetColumnIndexContainer = columnIndexContainerMap.get(_schema.getOffsetKey());
    _upsertWaterMarkManager = UpsertWaterMarkManager.getInstance();
    for (Map.Entry<String, ColumnIndexContainer> entry: columnIndexContainerMap.entrySet()) {
      String columnName = entry.getKey();
      ColumnIndexContainer container = entry.getValue();
      if (_schema.isVirtualColumn(columnName) && (container.getForwardIndex() instanceof VirtualColumnLongValueReaderWriter)) {
        _virtualColumnsReaderWriter.add((VirtualColumnLongValueReaderWriter) container.getForwardIndex());
      }
    }
    _sourceOffsetToDocId = buildOffsetToDocIdMap();
  }

  private ImmutableMap<Long, Integer> buildOffsetToDocIdMap() {
    final DataFileReader reader = _offsetColumnIndexContainer.getForwardIndex();
    final Dictionary dictionary = _offsetColumnIndexContainer.getDictionary();
    ImmutableMap.Builder<Long, Integer> kafkaOffsetToDocIdBuilder = ImmutableMap.builder();
    if (reader instanceof BaseSingleColumnSingleValueReader) {
      BaseSingleColumnSingleValueReader scsvReader = (BaseSingleColumnSingleValueReader) reader;
      for (int docId = 0; docId < _totalDoc; docId++) {
        final Long offset;
        if (dictionary == null) {
          offset = scsvReader.getLong(docId);
        } else {
          offset = (Long) dictionary.get(scsvReader.getInt(docId));
        }
        if (offset == null) {
          LOGGER.error("kafka offset is null at docID {}", docId);
        } else {
          kafkaOffsetToDocIdBuilder.put(offset, docId);
        }
      }
      return kafkaOffsetToDocIdBuilder.build();
    } else {
      throw new RuntimeException("unexpected forward reader type for kafka offset column " + reader.getClass());
    }
  }

  public static ImmutableUpsertSegmentImpl copyOf(ImmutableSegmentImpl immutableSegment) {

    return new ImmutableUpsertSegmentImpl(immutableSegment._segmentDirectory, immutableSegment._segmentMetadata,
        immutableSegment._indexContainerMap, immutableSegment._starTreeIndexContainer);
  }

  @Override
  public void updateVirtualColumn(List<UpdateLogEntry> logEntryList) {
    for (UpdateLogEntry logEntry: logEntryList) {
      boolean updated = false;
      Integer docId = _sourceOffsetToDocId.get(logEntry.getOffset());
      if (docId == null) {
        LOGGER.warn("segment {} failed to found docId for log update entry {}", _segmentName, logEntry.toString());
      } else {
        for (VirtualColumnLongValueReaderWriter readerWriter : _virtualColumnsReaderWriter) {
          updated = readerWriter.update(docId, logEntry.getValue(), logEntry.getType()) || updated;
        }
        if (updated) {
          _upsertWaterMarkManager.processMessage(_tableNameWithType, _segmentName, logEntry);
        }
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
      for (VirtualColumnLongValueReaderWriter readerWriter : _virtualColumnsReaderWriter) {
        result.append(readerWriter.getLong(docId)).append("; ");
      }
    }
    return result.toString();
  }

  /**
   * this method will fetch all updates from update logs in local disk and apply those updates to the current virtual columns
   * it traverses through all records in the current segment and match existing updates log to its kafka offsets
   * @throws IOException
   */
  @Override
  public void initVirtualColumn() throws IOException {
    long start = System.currentTimeMillis();
    List<UpdateLogEntry> updateLogEntries = UpdateLogStorageProvider.getInstance().getAllMessages(_tableNameWithType, _segmentName);
    Multimap<Long, UpdateLogEntry> updateLogEntryMap = ArrayListMultimap.create();
    for (UpdateLogEntry logEntry: updateLogEntries) {
      updateLogEntryMap.put(logEntry.getOffset(), logEntry);
    }
    for (Map.Entry<Long, Integer> mapEntry: _sourceOffsetToDocId.entrySet()) {
      final long offset = mapEntry.getKey();
      final int docId = mapEntry.getValue();
      if (updateLogEntryMap.containsKey(offset)) {
        boolean updated = false;
        Collection<UpdateLogEntry> entries = updateLogEntryMap.get(offset);
        UpdateLogEntry lastEntry = null;
        for (UpdateLogEntry entry : entries) {
          lastEntry = entry;
          for (VirtualColumnLongValueReaderWriter readerWriter : _virtualColumnsReaderWriter) {
            updated = readerWriter.update(docId, entry.getValue(), entry.getType()) || updated;
          }
        }
        if (updated) {
          _upsertWaterMarkManager.processMessage(_tableNameWithType, _segmentName, lastEntry);
        }
      }
    }
    LOGGER.info("loaded {} update log entries for current immutable segment {} in {} ms", updateLogEntries.size(),
        _segmentName, System.currentTimeMillis() - start);
  }
}

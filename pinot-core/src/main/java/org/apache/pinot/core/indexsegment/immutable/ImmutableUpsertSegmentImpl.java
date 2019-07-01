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

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.pinot.core.indexsegment.UpsertSegment;
import org.apache.pinot.core.segment.updater.UpsertWaterMarkManager;
import org.apache.pinot.opal.common.StorageProvider.UpdateLogEntry;
import org.apache.pinot.opal.common.StorageProvider.UpdateLogStorageProvider;
import org.apache.pinot.core.segment.virtualcolumn.mutable.VirtualColumnLongValueReaderWriter;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.core.io.reader.BaseSingleColumnSingleValueReader;
import org.apache.pinot.core.io.reader.DataFileReader;
import org.apache.pinot.core.segment.index.SegmentMetadataImpl;
import org.apache.pinot.core.segment.index.column.ColumnIndexContainer;
import org.apache.pinot.core.segment.index.readers.BitmapInvertedIndexReader;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;
import org.apache.pinot.core.segment.store.SegmentDirectory;
import org.apache.pinot.core.startree.v2.store.StarTreeIndexContainer;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
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
  private final ColumnIndexContainer _kafkaOffsetColumnIndexContainer;
  private final UpsertWaterMarkManager _upsertWaterMarkManager;

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
    _kafkaOffsetColumnIndexContainer = columnIndexContainerMap.get(_schema.getOffsetKey());
    _upsertWaterMarkManager = UpsertWaterMarkManager.getInstance();
    for (Map.Entry<String, ColumnIndexContainer> entry: columnIndexContainerMap.entrySet()) {
      String columnName = entry.getKey();
      ColumnIndexContainer container = entry.getValue();
      if (_schema.isVirtualColumn(columnName) && (container.getForwardIndex() instanceof VirtualColumnLongValueReaderWriter)) {
        _virtualColumnsReaderWriter.add((VirtualColumnLongValueReaderWriter) container.getForwardIndex());
      }
    }
  }

  public static ImmutableUpsertSegmentImpl copyOf(ImmutableSegmentImpl immutableSegment) {

    return new ImmutableUpsertSegmentImpl(immutableSegment._segmentDirectory, immutableSegment._segmentMetadata,
        immutableSegment._indexContainerMap, immutableSegment._starTreeIndexContainer);
  }

  @Override
  public void updateVirtualColumn(List<UpdateLogEntry> logEntryList) {
    for (UpdateLogEntry logEntry: logEntryList) {
      ImmutableRoaringBitmap bitmap = getDocIdsForOffset(logEntry.getOffset());
      if (bitmap == null) {
        LOGGER.info("offset {} not found while trying to update", logEntry.getOffset());
      } else {
        IntIterator it = bitmap.getIntIterator();
        boolean updated = false;
        while (it.hasNext()) {
          int docId = it.next();
          for (VirtualColumnLongValueReaderWriter readerWriter: _virtualColumnsReaderWriter) {
            updated = readerWriter.update(docId, logEntry.getValue(), logEntry.getType()) || updated;
          }
        }
        if (updated) {
          _upsertWaterMarkManager.processMessage(_tableNameWithType, _segmentName, logEntry);
        }
      }
    }
  }

  private ImmutableRoaringBitmap getDocIdsForOffset(long offset) {
    int dictionaryId = _kafkaOffsetColumnIndexContainer.getDictionary().indexOf(offset);
    if (dictionaryId < 0) {
      return null;
    }
    InvertedIndexReader invertedIndexReader= _kafkaOffsetColumnIndexContainer.getInvertedIndex();
    if (invertedIndexReader instanceof BitmapInvertedIndexReader) {
      return ((BitmapInvertedIndexReader) invertedIndexReader).getDocIds(dictionaryId);
    } else {
      throw new RuntimeException("unexpected data type for kafka offset column inverted index reader "
          + invertedIndexReader.getClass().toString());
    }
  }

  /**
   * this method will fetch all updates from update logs in local disk and apply those updates to the current virtual columns
   * it traverse through all records in current segment and match existing updates log to its kafka offsets
   * @throws IOException
   */
  @Override
  public void initVirtualColumn() throws IOException {
    List<UpdateLogEntry> updateLogEntries = UpdateLogStorageProvider.getInstance().getAllMessages(_tableNameWithType, _segmentName);
    LOGGER.info("got {} update log entries for current segment {}", updateLogEntries.size(), _segmentName);
    Multimap<Long, UpdateLogEntry> updateLogEntryMap = ArrayListMultimap.create();
    for (UpdateLogEntry logEntry: updateLogEntries) {
      updateLogEntryMap.put(logEntry.getOffset(), logEntry);
    }

    // go through all records and update the value
    final DataFileReader reader = _kafkaOffsetColumnIndexContainer.getForwardIndex();
    final Dictionary dictionary = _kafkaOffsetColumnIndexContainer.getDictionary();
    if (reader instanceof BaseSingleColumnSingleValueReader) {
      for (int i = 0; i < _totalDoc; i++) {
        long offset = (Long) dictionary.get(((BaseSingleColumnSingleValueReader) reader).getInt(i));
        if (updateLogEntryMap.containsKey(offset)) {
          boolean updated = false;
          Collection<UpdateLogEntry> entries = updateLogEntryMap.get(offset);
          UpdateLogEntry lastEntry = null;
          for (UpdateLogEntry entry : entries) {
            lastEntry = entry;
            for (VirtualColumnLongValueReaderWriter readerWriter : _virtualColumnsReaderWriter) {
              updated = readerWriter.update(i, entry.getValue(), entry.getType()) || updated;
            }
          }
          if (updated) {
            _upsertWaterMarkManager.processMessage(_tableNameWithType, _segmentName, lastEntry);
          }
        }
      }
    } else {
      throw new RuntimeException("unexpected forward reader type for kafka offset column " + reader.getClass());
    }

  }
}

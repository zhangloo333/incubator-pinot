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
package org.apache.pinot.core.data.manager.upsert;

import org.apache.pinot.common.segment.SegmentMetadata;
import org.apache.pinot.core.io.reader.DataFileReader;
import org.apache.pinot.core.segment.index.column.ColumnIndexContainer;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogEntry;
import org.apache.pinot.spi.data.readers.GenericRow;

import java.io.IOException;
import java.util.Map;

/**
 * callback for handling any upsert-related operations in subclass of
 * {@link org.apache.pinot.core.indexsegment.IndexSegment} if necessary
 */
public interface IndexSegmentCallback {

  /**
   * initialize the callback from {@link org.apache.pinot.core.indexsegment.IndexSegment}
   * @param segmentMetadata the metadata associated with the curreng segment
   * @param virtualColumnIndexReader
   */
  void init(SegmentMetadata segmentMetadata, Map<String, DataFileReader> virtualColumnIndexReader);

  /**
   * initialize offset column for in-memory access
   * @param offsetColumnContainer the column that stores the offset data
   */
  void initOffsetColumn(ColumnIndexContainer offsetColumnContainer);

  /**
   * perform any operation from the callback for the given row after it has been processed and index
   * @param row the current pinot row we just indexed into the current IndexSegment
   * @param docId the docId of this record
   */
  void postProcessRecords(GenericRow row, int docId);

  /**
   * initialize set of upsert-related virtual columns if necessary
   * @throws IOException
   */
  void initVirtualColumn() throws IOException;

  /**
   * update upsert-related virtual column from segment updater if necessary
   * @param logEntries
   */
  void updateVirtualColumn(Iterable<UpdateLogEntry> logEntries);

  /**
   * retrieve a information related to an upsert-enable segment virtual column for debug purpose
   * @param offset the offset of the record we are trying to get the virtual columnn data for
   * @return string representation of the virtual column data information
   */
  String getVirtualColumnInfo(long offset);
}

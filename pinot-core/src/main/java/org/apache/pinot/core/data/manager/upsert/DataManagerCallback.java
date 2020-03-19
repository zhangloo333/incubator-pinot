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

import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogEntry;
import org.apache.pinot.spi.data.readers.GenericRow;

import java.io.IOException;
import java.util.List;

/**
 * component inject to {@link org.apache.pinot.core.data.manager.SegmentDataManager} for handling extra logics for
 * other workflows other than regular append-mode ingestion. We are expected to provide appropriate link to class
 * during run time
 */
public interface DataManagerCallback {

  /**
   * create a callback component for {@link org.apache.pinot.core.indexsegment.IndexSegment} when
   * {@link org.apache.pinot.core.data.manager.SegmentDataManager} create one.
   * @return callback associated with the internal index segment this data manager holds
   */
  IndexSegmentCallback getIndexSegmentCallback();

  /**
   * process the row after transformation in the ingestion process
   * @param row the row of newly ingested and transformed data from upstream
   * @param offset the offset of this particular row
   */
  void processTransformedRow(GenericRow row, long offset);

  /**
   * process the row after we have finished the index the current row
   * @param row the row we just index to the current segment
   * @param offset the offset associated with the index
   */
  void postIndexProcessing(GenericRow row, long offset);

  /**
   * callback for when a realtime segment data manager done with the current consumption loop for all data associated
   * with it
   */
  void postConsumeLoop();

  /**
   * initialize all virtual columns for the current data manager associated with upsert component (if necessary)
   * @throws IOException
   */
  void initVirtualColumns() throws IOException;

  /**
   * update the data in the virtual columns from segment updater loop if necessary
   * @param messages list of update log entries for the current datamanager
   */
  void updateVirtualColumns(List<UpdateLogEntry> messages);

  /**
   * callback when the associated data manager is destroyed by pinot server in call {@link SegmentDataManager#destroy()}
   */
  void destroy();
}

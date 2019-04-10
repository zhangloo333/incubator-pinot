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
package com.linkedin.pinot.core.data.manager.offline;

import com.linkedin.pinot.core.data.manager.UpsertSegmentDataManager;
import com.linkedin.pinot.core.indexsegment.UpsertSegment;
import com.linkedin.pinot.core.segment.updater.SegmentUpdater;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import com.linkedin.pinot.opal.common.StorageProvider.UpdateLogEntry;

import java.io.IOException;
import java.util.List;

public class UpsertImmutableSegmentDataManager extends ImmutableSegmentDataManager implements UpsertSegmentDataManager {

  private String _tableNameWithType;

  public UpsertImmutableSegmentDataManager(ImmutableSegment immutableSegment) throws IOException {
    super(immutableSegment);
    _tableNameWithType = TableNameBuilder.ensureTableNameWithType(immutableSegment.getSegmentMetadata().getTableName(),
        CommonConstants.Helix.TableType.REALTIME);
    initVirtualColumns();
  }

  @Override
  public void updateVirtualColumn(List<UpdateLogEntry> messages) {
    ((UpsertSegment) _immutableSegment).updateVirtualColumn(messages);
  }

  @Override
  public void destroy() {
    SegmentUpdater.getInstance().removeSegmentDataManager(_tableNameWithType, getSegmentName(), this);
    super.destroy();
  }

  private void initVirtualColumns() throws IOException {
    // 1. ensure the data manager can capture all update events
    // 2. load all existing messages
    SegmentUpdater.getInstance().addSegmentDataManager(_tableNameWithType, new LLCSegmentName(getSegmentName()), this);
    ((UpsertSegment) _immutableSegment).initVirtualColumn();
  }
}

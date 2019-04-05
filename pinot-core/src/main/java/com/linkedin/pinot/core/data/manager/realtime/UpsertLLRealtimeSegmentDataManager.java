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
package com.linkedin.pinot.core.data.manager.realtime;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.data.manager.UpsertSegmentDataManager;
import com.linkedin.pinot.core.indexsegment.UpsertSegment;
import com.linkedin.pinot.core.indexsegment.mutable.MutableUpsertSegmentImpl;
import com.linkedin.pinot.core.segment.updater.SegmentUpdater;
import com.linkedin.pinot.core.segment.virtualcolumn.StorageProvider.UpdateLogEntry;
import com.linkedin.pinot.opal.common.RpcQueue.ProduceTask;
import com.linkedin.pinot.opal.common.messages.KeyCoordinatorMessageContext;
import com.linkedin.pinot.opal.common.messages.KeyCoordinatorQueueMsg;
import com.linkedin.pinot.opal.distributed.keyCoordinator.serverIngestion.KeyCoordinatorProvider;
import com.linkedin.pinot.opal.distributed.keyCoordinator.serverIngestion.KeyCoordinatorQueueProducer;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.data.DimensionFieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.data.TimeFieldSpec;
import org.apache.pinot.common.metadata.instance.InstanceZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.data.manager.realtime.LLRealtimeSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.core.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.core.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;

import java.io.IOException;
import java.util.List;

/**
 * class design is pretty bad right now, need to rework inheritance to abstract the base class or use composition instead
 */
public class UpsertLLRealtimeSegmentDataManager extends LLRealtimeSegmentDataManager implements UpsertSegmentDataManager {

  private final KeyCoordinatorQueueProducer _keyCoordinatorQueueProducer;

  public UpsertLLRealtimeSegmentDataManager(RealtimeSegmentZKMetadata segmentZKMetadata, TableConfig tableConfig, InstanceZKMetadata instanceZKMetadata, RealtimeTableDataManager realtimeTableDataManager, String resourceDataDir, IndexLoadingConfig indexLoadingConfig, Schema schema, ServerMetrics serverMetrics) throws Exception {
    super(segmentZKMetadata, tableConfig, instanceZKMetadata, realtimeTableDataManager, resourceDataDir, indexLoadingConfig, schema, serverMetrics);
    Preconditions.checkState(_schema.getPrimaryKeyFieldSpec() != null, "primary key not found");
    Preconditions.checkState(_schema.getOffsetKeyFieldSpec() != null, "offset key not found");

    _keyCoordinatorQueueProducer = KeyCoordinatorProvider.getInstance().getProducer();
    initVirtualColumns();
  }

  public String getSegmentName() {
    return _segmentNameStr;
  }

  @Override
  public void updateVirtualColumn(List<UpdateLogEntry> messages) {
    ((UpsertSegment) _realtimeSegment).updateVirtualColumn(messages);
  }

  @Override
  public void destroy() {
    SegmentUpdater.getInstance().removeSegmentDataManager(_tableNameWithType, _segmentName.getSegmentName(), this);
    super.destroy();
  }

  @Override
  protected MutableSegmentImpl createMutableSegment(RealtimeSegmentConfig config) {
    return new MutableUpsertSegmentImpl(config);
  }

  @Override
  protected void processTransformedRow(GenericRow row, long offset) {
    row.putField(_schema.getOffsetKey(), offset);
    super.processTransformedRow(row, offset);
  }

  @Override
  protected void postIndexProcessing(GenericRow row, long offset) {
    emitEventToKeyCoordinator(row, offset);
    super.postIndexProcessing(row, offset);
  }

  @Override
  protected boolean consumeLoop() throws Exception {
    boolean result = super.consumeLoop();
    segmentLogger.info("flushing kafka producer");
    _keyCoordinatorQueueProducer.flush();
    segmentLogger.info("done flushing kafka producer");
    return result;
  }

  /**
   * not handling error right now
   * @param row
   * @param offset
   */
  private void emitEventToKeyCoordinator(GenericRow row, long offset) {
    final byte[] primaryKeyBytes = getPrimaryKeyBytesFromRow(row);
    final long timestampMillis = getTimestampFromRow(row);
    ProduceTask<Integer, KeyCoordinatorQueueMsg> task = new ProduceTask<>(_streamPartitionId, new KeyCoordinatorQueueMsg(_tableNameWithType,
        primaryKeyBytes, new KeyCoordinatorMessageContext(_segmentNameStr, timestampMillis, offset)));
    _keyCoordinatorQueueProducer.produceSync(task);
  }

  private byte[] getPrimaryKeyBytesFromRow(GenericRow row) {
    DimensionFieldSpec primaryKeyDimension = _schema.getPrimaryKeyFieldSpec();
    return _schema.getByteArrayFromField(row.getValue(primaryKeyDimension.getName()), primaryKeyDimension);
  }

  private long getTimestampFromRow(GenericRow row) {
    TimeFieldSpec spec = _schema.getTimeFieldSpec();
    return spec.getIncomingGranularitySpec().toMillis(row.getValue(spec.getIncomingTimeColumnName()));
  }


  private void initVirtualColumns() throws IOException {
    // 1. ensure the data manager can capture all update events
    // 2. load all existing messages
    SegmentUpdater.getInstance().addSegmentDataManager(_tableNameWithType, _segmentName, this);
    ((UpsertSegment) _realtimeSegment).initVirtualColumn();
  }
}

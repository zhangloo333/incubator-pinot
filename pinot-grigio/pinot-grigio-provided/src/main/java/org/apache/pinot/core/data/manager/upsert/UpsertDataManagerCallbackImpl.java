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

import com.google.common.base.Preconditions;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.segment.updater.SegmentUpdater;
import org.apache.pinot.grigio.common.messages.KeyCoordinatorQueueMsg;
import org.apache.pinot.grigio.common.rpcQueue.ProduceTask;
import org.apache.pinot.grigio.common.rpcQueue.QueueProducer;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogEntry;
import org.apache.pinot.grigio.servers.KeyCoordinatorProvider;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class UpsertDataManagerCallbackImpl implements DataManagerCallback {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertDataManagerCallbackImpl.class);

  private final String _tableNameWithType;
  private final QueueProducer _keyCoordinatorQueueProducer;
  private final String _segmentName;
  private final Schema _schema;
  private final ServerMetrics _serverMetrics;
  private IndexSegmentCallback _indexSegmentCallback;

  public UpsertDataManagerCallbackImpl(String tableNameWithType, String segmentName, Schema schema,
      ServerMetrics serverMetrics, boolean isMutable) {
    Preconditions.checkState(schema.getPrimaryKeyFieldSpec() != null, "primary key not found");
    Preconditions.checkState(schema.getOffsetKeyFieldSpec() != null, "offset key not found");
    _tableNameWithType = TableNameBuilder.ensureTableNameWithType(tableNameWithType,
        CommonConstants.Helix.TableType.REALTIME);
    _segmentName = segmentName;
    _schema = schema;
    _serverMetrics = serverMetrics;
    _keyCoordinatorQueueProducer = KeyCoordinatorProvider.getInstance().getCachedProducer(_tableNameWithType);
    if (isMutable) {
      _indexSegmentCallback = new UpsertMutableIndexSegmentCallback();
    } else {
      _indexSegmentCallback = new UpsertImmutableIndexSegmentCallback();
    }
  }

  @Override
  public synchronized IndexSegmentCallback getIndexSegmentCallback() {
    return _indexSegmentCallback;
  }

  @Override
  public void processTransformedRow(GenericRow row, long offset) {
    row.putValue(_schema.getOffsetKey(), offset);
  }

  @Override
  public void postIndexProcessing(GenericRow row, long offset) {
    emitEventToKeyCoordinator(row, offset);
  }

  @Override
  public void postConsumeLoop() {
    _keyCoordinatorQueueProducer.flush();
  }

  public void updateVirtualColumns(List<UpdateLogEntry> messages) {
    _indexSegmentCallback.updateVirtualColumn(messages);
  }

  public String getVirtualColumnInfo(long offset) {
    return _indexSegmentCallback.getVirtualColumnInfo(offset);
  }

  public void destroy() {
    try {
      SegmentUpdater.getInstance().removeSegmentDataManager(_tableNameWithType, _segmentName, this);
    } catch (Exception ex) {
      LOGGER.error("failed to destroy data manager callback");
    }
  }

  /**
   * not handling error right now
   * @param row
   * @param offset
   */
  private void emitEventToKeyCoordinator(GenericRow row, long offset) {
    final byte[] primaryKeyBytes = getPrimaryKeyBytesFromRow(row);
    final long timestampMillis = getTimestampFromRow(row);
    ProduceTask<byte[], KeyCoordinatorQueueMsg> task = new ProduceTask<>(primaryKeyBytes,
        new KeyCoordinatorQueueMsg(primaryKeyBytes, _segmentName, timestampMillis, offset));
    task.setCallback(new ProduceTask.Callback() {
      @Override
      public void onSuccess() {
        // do nothing on success
      }

      @Override
      public void onFailure(Exception ex) {
        // right now we just log the error and not really doing anything
        // TODO: retries/record logics
        _serverMetrics.addMeteredGlobalValue(ServerMeter.MESSAGE_PRODUCE_FAILED_COUNT, 1);
      }
    });
    _keyCoordinatorQueueProducer.produce(task);
  }

  private byte[] getPrimaryKeyBytesFromRow(GenericRow row) {
    DimensionFieldSpec primaryKeyDimension = _schema.getPrimaryKeyFieldSpec();
    return _schema.getByteArrayFromField(row.getValue(primaryKeyDimension.getName()), primaryKeyDimension);
  }

  private long getTimestampFromRow(GenericRow row) {
    TimeFieldSpec spec = _schema.getTimeFieldSpec();
    return spec.getIncomingGranularitySpec().toMillis(row.getValue(spec.getIncomingTimeColumnName()));
  }

  @Override
  public void initVirtualColumns() throws IOException {
    // 1. ensure the data manager can capture all update events
    // 2. load all existing messages
    SegmentUpdater.getInstance().addSegmentDataManager(_tableNameWithType, new LLCSegmentName(_segmentName), this);
    _indexSegmentCallback.initVirtualColumn();
  }

}

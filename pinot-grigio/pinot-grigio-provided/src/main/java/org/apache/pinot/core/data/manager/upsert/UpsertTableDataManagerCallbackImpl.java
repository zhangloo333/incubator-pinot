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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogStorageProvider;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * class for handle all upsert related operation for interacting with segments for a given table at
 * {@link org.apache.pinot.core.data.manager.TableDataManager}
 */
public class UpsertTableDataManagerCallbackImpl implements TableDataManagerCallback {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertTableDataManagerCallbackImpl.class);

  private UpdateLogStorageProvider _updateLogStorageProvider;

  @Override
  public void init() {
    _updateLogStorageProvider = UpdateLogStorageProvider.getInstance();
  }

  @Override
  public void addSegment(String tableName, String segmentName, TableConfig tableConfig) {
    try {
      _updateLogStorageProvider.addSegment(tableName, segmentName);
    } catch (IOException e) {
      LOGGER.error("failed to add update log for segment {} {}", tableName, segmentName);
      ExceptionUtils.rethrow(e);
    }
  }

  @Override
  public DataManagerCallback getMutableDataManagerCallback(String tableNameWithType, String segmentName, Schema schema,
      ServerMetrics serverMetrics) {
    return getDataManagerCallback(tableNameWithType, segmentName, schema, serverMetrics, true);
  }

  @Override
  public DataManagerCallback getImmutableDataManagerCallback(String tableNameWithType, String segmentName,
      Schema schema, ServerMetrics serverMetrics) {
    return getDataManagerCallback(tableNameWithType, segmentName, schema, serverMetrics, false);
  }

  private DataManagerCallback getDataManagerCallback(String tableName, String segmentName, Schema schema,
      ServerMetrics serverMetrics, boolean isMutable) {
    return new UpsertDataManagerCallbackImpl(tableName, segmentName, schema, serverMetrics, isMutable);
  }

  @Override
  public DataManagerCallback getDefaultDataManagerCallback() {
    throw new NotImplementedException("cannot create DefaultDataManagerCallback from UpsertTableDataManagerCallback");
  }
}

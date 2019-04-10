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

import com.linkedin.pinot.core.data.manager.offline.UpsertImmutableSegmentDataManager;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.metadata.instance.InstanceZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.LLRealtimeSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.opal.common.StorageProvider.UpdateLogStorageProvider;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Semaphore;

public class UpsertRealtimeTableDataManager extends RealtimeTableDataManager {
  private UpdateLogStorageProvider _updateLogStorageProvider;

  public UpsertRealtimeTableDataManager(Semaphore segmentBuildSemaphore) {
    super(segmentBuildSemaphore);
    _updateLogStorageProvider = UpdateLogStorageProvider.getInstance();
  }

  @Override
  // for adding
  public void addSegment(@Nonnull String segmentName, @Nonnull TableConfig tableConfig,
                         @Nonnull IndexLoadingConfig indexLoadingConfig) throws Exception {
    _updateLogStorageProvider.addSegment(_tableNameWithType, segmentName);
    super.addSegment(segmentName, tableConfig, indexLoadingConfig);
  }

  @Override
  protected LLRealtimeSegmentDataManager getLLRealtimeSegmentDataManager(RealtimeSegmentZKMetadata realtimeSegmentZKMetadata,
                                                                         TableConfig tableConfig,
                                                                         InstanceZKMetadata instanceZKMetadata,
                                                                         RealtimeTableDataManager realtimeTableDataManager,
                                                                         String indexDirPath,
                                                                         IndexLoadingConfig indexLoadingConfig,
                                                                         Schema schema, ServerMetrics serverMetrics)
      throws Exception {
    return new UpsertLLRealtimeSegmentDataManager(realtimeSegmentZKMetadata, tableConfig, instanceZKMetadata,
        realtimeTableDataManager, indexDirPath, indexLoadingConfig, schema, serverMetrics);
  }

  @Override
  protected ImmutableSegmentDataManager getImmutableSegmentDataManager(ImmutableSegment immutableSegment) {
    try {
      return new UpsertImmutableSegmentDataManager(immutableSegment);
    } catch (IOException e) {
      throw new RuntimeException("failed to init the upsert immutable segment", e);
    }
  }

  @Override
  protected ImmutableSegment loadImmutableSegment(File indexDir, IndexLoadingConfig indexLoadingConfig) {
    try {
      return ImmutableSegmentLoader.loadUpsertSegment(indexDir, indexLoadingConfig);
    } catch (Exception e) {
      throw new RuntimeException("failed to load immutable segment", e);
    }
  }

}

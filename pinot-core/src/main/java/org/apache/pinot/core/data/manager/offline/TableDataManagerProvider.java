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
package org.apache.pinot.core.data.manager.offline;

import com.linkedin.pinot.core.data.manager.realtime.UpsertRealtimeTableDataManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.core.data.manager.TableDataManager;
import org.apache.pinot.core.data.manager.config.InstanceDataManagerConfig;
import org.apache.pinot.core.data.manager.config.TableDataManagerConfig;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.Semaphore;


/**
 * Factory for {@link TableDataManager}.
 */
public class TableDataManagerProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableDataManagerProvider.class);

  private static Semaphore _segmentBuildSemaphore;

  private TableDataManagerProvider() {
  }

  public static void init(InstanceDataManagerConfig instanceDataManagerConfig) {
    int maxParallelBuilds = instanceDataManagerConfig.getMaxParallelSegmentBuilds();
    if (maxParallelBuilds > 0) {
      _segmentBuildSemaphore = new Semaphore(maxParallelBuilds, true);
    }
  }

  public static TableDataManager getTableDataManager(@Nonnull TableDataManagerConfig tableDataManagerConfig,
      @Nonnull String instanceId, @Nonnull ZkHelixPropertyStore<ZNRecord> propertyStore,
      @Nonnull ServerMetrics serverMetrics) {
    TableDataManager tableDataManager;
    switch (CommonConstants.Helix.TableType.valueOf(tableDataManagerConfig.getTableDataManagerType())) {
      case OFFLINE:
        tableDataManager = new OfflineTableDataManager();
        break;
      case REALTIME:
        if (tableDataManagerConfig.getUpdateSemantic() == CommonConstants.UpdateSemantic.UPSERT) {
          LOGGER.info("getting upsert table manager");
          tableDataManager = new UpsertRealtimeTableDataManager(_segmentBuildSemaphore);
        } else {
          LOGGER.info("getting regular realtime table manager");
          tableDataManager = new RealtimeTableDataManager(_segmentBuildSemaphore);
        }
        break;
      default:
        throw new IllegalStateException();
    }
    tableDataManager.init(tableDataManagerConfig, instanceId, propertyStore, serverMetrics);
    return tableDataManager;
  }
}

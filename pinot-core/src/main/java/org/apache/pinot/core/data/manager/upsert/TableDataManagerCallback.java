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

import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.spi.data.Schema;

import java.io.File;

/**
 * component inject to {@link org.apache.pinot.core.data.manager.TableDataManager} for handling extra logics for
 * other workflows other than regular append-mode ingestion. We are expected to provide appropriate link to class
 * during run time
 */
public interface TableDataManagerCallback {

  /**
   * initialize the callback object during {@link org.apache.pinot.core.data.manager.TableDataManager#init}
   * ensure any internal component for this callback is properly created during the start time
   */
  void init();

  /**
   * callback to ensure other components related to the callback are added when
   * {@link org.apache.pinot.core.data.manager.TableDataManager#addSegment(File, IndexLoadingConfig)}
   * is executed
   */
  void addSegment(String tableNameWithType, String segmentName, TableConfig tableConfig);

  /**
   * return a callback object for an Immutable segment data manager callback component when a table create a new
   * immutable {@link org.apache.pinot.core.data.manager.SegmentDataManager}
   */
  DataManagerCallback getMutableDataManagerCallback(String tableNameWithType, String segmentName, Schema schema,
      ServerMetrics serverMetrics);

  /**
   * return a callback object for a mutable segment data manager callback component when a table create a new
   * immutable {@link org.apache.pinot.core.data.manager.SegmentDataManager}
   */
  DataManagerCallback getImmutableDataManagerCallback(String tableNameWithType, String segmentName, Schema schema,
      ServerMetrics serverMetrics);

  /**
   * create a no-op default callback for segmentDataManager that don't support upsert
   * (eg, offline table, HLL consumers etc)
   * @return a no-op default callback for data manager
   */
  DataManagerCallback getDefaultDataManagerCallback();
}

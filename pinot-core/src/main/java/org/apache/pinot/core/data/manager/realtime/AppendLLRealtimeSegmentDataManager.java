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
package org.apache.pinot.core.data.manager.realtime;

import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.metadata.instance.InstanceZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.indexsegment.mutable.MutableAppendSegmentImpl;
import org.apache.pinot.core.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.core.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;

public class AppendLLRealtimeSegmentDataManager extends LLRealtimeSegmentDataManager {

  public AppendLLRealtimeSegmentDataManager(RealtimeSegmentZKMetadata segmentZKMetadata, TableConfig tableConfig, InstanceZKMetadata instanceZKMetadata, RealtimeTableDataManager realtimeTableDataManager, String resourceDataDir, IndexLoadingConfig indexLoadingConfig, Schema schema, ServerMetrics serverMetrics) throws Exception {
    super(segmentZKMetadata, tableConfig, instanceZKMetadata, realtimeTableDataManager, resourceDataDir, indexLoadingConfig, schema, serverMetrics);
  }

  @Override
  protected MutableSegmentImpl createMutableSegment(RealtimeSegmentConfig config) {
    return new MutableAppendSegmentImpl(config);
  }

  @Override
  protected void processTransformedRow(GenericRow row, long offset) {
    // do nothing
  }

  @Override
  protected void postIndexProcessing(GenericRow row, long offset) {
    // do nothing
  }

}

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
package org.apache.pinot.core.segment.updater;

import org.apache.commons.configuration.Configuration;
import org.apache.pinot.grigio.common.metrics.GrigioMetrics;

import java.util.Map;

/**
 * class run on pinot server to keep track of the low-water-mark of each upsert table
 * organized by partition
 */
public interface WaterMarkManager {

  /**
   * initialize watermark manager
   * @param config the configuration subset for waterMarkManager
   * @param metrics the metrics for watermark manager
   */
  void init(Configuration config, GrigioMetrics metrics);

  /**
   * the highest epoch for each partition of each table in this pinot server
   * @return mapping of {pinot_table_name: {partition_id: high_water_mark}}
   * example as {
   *     "table1_REALTIME" : {
   *       "0" : 1400982,
   *       "1" : 1400982,
   *       "2" : 1400982,
   *       "3" : 1400982
   *     },
   *     "table2_REALTIME" : {
   *       "0" : 1401008,
   *       "1" : 1401008
   *     }
   */
  Map<String, Map<Integer, Long>> getHighWaterMarkTablePartitionMap();
}

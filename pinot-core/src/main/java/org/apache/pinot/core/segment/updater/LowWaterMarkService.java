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

import org.apache.helix.HelixDataAccessor;
import org.apache.pinot.common.metrics.BrokerMetrics;

import java.util.Map;

/**
 * LowWaterMarkService keeps records of the low water mark (i.e., the stream ingestion progress) for each partition of
 * an input table.
 * It runs on pinot broker to fetch lwm information from pinot server periodically
 * and use that to rewrite pinot query periodically
 */
public interface LowWaterMarkService {

  void init(HelixDataAccessor helixDataAccessor, String helixClusterName, int serverPollingInterval, int serverPort);

  /**
   * the low water mark mapping from partition id to the corresponding low water mark of a given table.
   * @param tableNameWithType
   * @return map of partition to lowWatermark
   */
  Map<Integer, Long> getLowWaterMarks(String tableNameWithType);

  /**
   * shutdown low water mark service and its background threads (if any)
   */
  void shutDown();

  /**
   * start the current low watermark service
   * @param brokerMetrics pinot broker metrics for lwm service to report its status to
   */
  void start(BrokerMetrics brokerMetrics);

  /**
   * get a queryrewriter to ensure that we can rewrite a query if the target table is upsert-enabled table
   * @return
   */
  QueryRewriter getQueryRewriter();
}

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
package org.apache.pinot.broker.upsert;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixDataAccessor;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.core.segment.updater.LowWaterMarkService;
import org.apache.pinot.core.segment.updater.QueryRewriter;

import java.util.Map;

/**
 * default class to handle any low watermark operation on pinot broker, mostly no-op
 */
public class DefaultLowWaterMarkService implements LowWaterMarkService {

  private QueryRewriter queryRewriter = new DefaultUpsertQueryRewriter();

  @Override
  public void init(HelixDataAccessor helixDataAccessor, String helixClusterName, int serverPollingInterval,
      int serverPort){
  }

  @Override
  public Map<Integer, Long> getLowWaterMarks(String tableName) {
    return ImmutableMap.of();
  }

  @Override
  public void shutDown() {
  }

  @Override
  public void start(BrokerMetrics brokerMetrics) {
  }

  @Override
  public QueryRewriter getQueryRewriter() {
    return queryRewriter;
  }
}

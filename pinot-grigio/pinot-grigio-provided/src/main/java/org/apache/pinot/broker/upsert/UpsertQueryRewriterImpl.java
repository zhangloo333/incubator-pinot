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

import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.FilterQuery;
import org.apache.pinot.common.request.FilterQueryMap;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.core.segment.updater.LowWaterMarkService;
import org.apache.pinot.core.segment.updater.QueryRewriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class UpsertQueryRewriterImpl implements QueryRewriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertQueryRewriterImpl.class);

  protected final LowWaterMarkService _lwmService;
  private static final String VALID_FROM = "$validFrom";
  private static final String VALID_UNTIL = "$validUntil";
  // Normal Pinot query node uses positive IDs. So lwm query node ids are all negative.
  private static final int QUERY_ID_BASE = -1000;

  public UpsertQueryRewriterImpl(LowWaterMarkService lwmService) {
    _lwmService = lwmService;
  }

  @Override
  public void maybeRewriteQueryForUpsert(BrokerRequest request, String rawTableName) {
    final String realtimeTableName = TableNameBuilder.ensureTableNameWithType(rawTableName,
        CommonConstants.Helix.TableType.REALTIME);
    Map<Integer, Long> lowWaterMarks = _lwmService.getLowWaterMarks(realtimeTableName);
    if (lowWaterMarks == null || lowWaterMarks.size() == 0) {
      LOGGER.info("No low water marks info found for table {}", realtimeTableName);
      return;
    }
    LOGGER.info("Found low water marks {} for table {}", String.valueOf(lowWaterMarks), realtimeTableName);
    addLowWaterMarkToQuery(request, lowWaterMarks);
    LOGGER.info("Query augmented with LWMS info for table {} : {}", realtimeTableName, request);
  }

  /**
   * For upsert enabled tables, augment the realtime query with low water mark constraints in its filter query of the
   * form
   *   ($validFrom <= lwm and $validFrom > -1) AND (lwm < $validUtil OR $validUtil = -1)
   *
   * @param realtimeBrokerRequest
   * @param lowWaterMarks
   */
  public void addLowWaterMarkToQuery(BrokerRequest realtimeBrokerRequest, Map<Integer, Long> lowWaterMarks) {
    if (lowWaterMarks == null || lowWaterMarks.size() == 0) {
      LOGGER.warn("No low water mark info found for query: {}", realtimeBrokerRequest);
      return;
    }

    // Choose the min lwm among all partitions.
    long minLwm = Collections.min(lowWaterMarks.values());

    // 1. Build the low water mark query of the form for a table assuming lwm is the min LWM and -1 is used as
    // uninitialized marker.
    // ($validFrom <= lwm and $validFrom > -1) AND (lwm < $validUtil OR $validUtil = -1)
    // -1 is used instead of Long.MAXVALUE because Pinot does not handle long arithmetic correctly.
    FilterQuery lwmQuery = addSinglePartitionLowWaterMark(QUERY_ID_BASE - 1, realtimeBrokerRequest, minLwm);

    // 2. Attach low water mark filter to the current filters.
    FilterQuery currentFilterQuery = realtimeBrokerRequest.getFilterQuery();
    if (currentFilterQuery != null) {
      // Make an AND query of lwmQuery and the existing query.
      FilterQuery andFilterQuery = new FilterQuery();
      // Make sure we do not reuse any query id in lwmQuerys.
      andFilterQuery.setId(QUERY_ID_BASE);
      andFilterQuery.setOperator(FilterOperator.AND);
      List<Integer> nestedFilterQueryIds = new ArrayList<>(2);
      nestedFilterQueryIds.add(currentFilterQuery.getId());
      nestedFilterQueryIds.add(lwmQuery.getId());
      andFilterQuery.setNestedFilterQueryIds(nestedFilterQueryIds);

      realtimeBrokerRequest.setFilterQuery(andFilterQuery);
      FilterQueryMap filterSubQueryMap = realtimeBrokerRequest.getFilterSubQueryMap();
      filterSubQueryMap.putToFilterQueryMap(lwmQuery.getId(), lwmQuery);
      filterSubQueryMap.putToFilterQueryMap(andFilterQuery.getId(), andFilterQuery);
    } else {
      realtimeBrokerRequest.getFilterSubQueryMap().putToFilterQueryMap(lwmQuery.getId(), lwmQuery);
      realtimeBrokerRequest.setFilterQuery(lwmQuery);
    }
  }

  /**
   *
   * @param queryIdBase The starting id that will be assigned to the first query created in ths method.
   * @param realtimeBrokerRequest
   * @param lwm low water mark.
   * @return a filter query corresponding to the low water mark constraint of a single partition. The general form is:
   *         ($ValidFrom <= lwm && $validFrom > -1)  AND (lwm < $validUtil OR $validUtil = -1)
   */
  private FilterQuery addSinglePartitionLowWaterMark(int queryIdBase, BrokerRequest realtimeBrokerRequest,
      Long lwm) {
    // ValidFromQuery: ($ValidFrom <= lwm && $validFrom > -1)
    FilterQuery validFromFilterQuery = new FilterQuery();
    // Important: Always decrement queryIdBase value after use to avoid id conflict.
    validFromFilterQuery.setId(queryIdBase--);
    validFromFilterQuery.setOperator(FilterOperator.AND);
    FilterQuery validFromP1 = getLeafFilterQuery(VALID_FROM, queryIdBase--, "(*\t\t" + lwm + "]", FilterOperator.RANGE, realtimeBrokerRequest);
    FilterQuery validFromP2 = getLeafFilterQuery(VALID_FROM, queryIdBase--, "(-1\t\t*)", FilterOperator.RANGE, realtimeBrokerRequest);
    List<Integer> nestedQueriesIdForValidFrom = new ArrayList<>();
    nestedQueriesIdForValidFrom.add(validFromP1.getId());
    nestedQueriesIdForValidFrom.add(validFromP2.getId());
    validFromFilterQuery.setNestedFilterQueryIds(nestedQueriesIdForValidFrom);

    // ValidUtilQuery: (lwm < $validUtil OR $validUtil = -1)
    FilterQuery validUtilFilterQuery = new FilterQuery();
    validUtilFilterQuery.setId(queryIdBase--);
    validUtilFilterQuery.setOperator(FilterOperator.OR);

    FilterQuery validUtilP1 = getLeafFilterQuery(VALID_UNTIL, queryIdBase--, "(" + lwm + "\t\t*)", FilterOperator.RANGE, realtimeBrokerRequest);
    FilterQuery validUtilP2 = getLeafFilterQuery(VALID_UNTIL, queryIdBase--, "-1", FilterOperator.EQUALITY, realtimeBrokerRequest);
    List<Integer> nestedQueriesIdForValidUtil = new ArrayList<>();
    nestedQueriesIdForValidUtil.add(validUtilP1.getId());
    nestedQueriesIdForValidUtil.add(validUtilP2.getId());
    validUtilFilterQuery.setNestedFilterQueryIds(nestedQueriesIdForValidUtil);

    // Top level query: ValidFromQuery AND ValidUtilQuery
    FilterQuery lwmQuery = new FilterQuery();
    lwmQuery.setId(queryIdBase--);
    lwmQuery.setOperator(FilterOperator.AND);
    List<Integer> nestQids = new ArrayList<>();
    nestQids.add(validFromFilterQuery.getId());
    nestQids.add(validUtilFilterQuery.getId());
    lwmQuery.setNestedFilterQueryIds(nestQids);

    // Add all the new created queries to the query map.
    realtimeBrokerRequest.getFilterSubQueryMap().putToFilterQueryMap(lwmQuery.getId(), lwmQuery);
    realtimeBrokerRequest.getFilterSubQueryMap().putToFilterQueryMap(validFromFilterQuery.getId(), validFromFilterQuery);
    realtimeBrokerRequest.getFilterSubQueryMap().putToFilterQueryMap(validUtilFilterQuery.getId(), validUtilFilterQuery);
    return lwmQuery;
  }

  private FilterQuery getLeafFilterQuery(String column, int id, String value, FilterOperator operator,
      BrokerRequest realtimeBrokerRequest) {
    FilterQuery filterQuery = new FilterQuery();
    filterQuery.setColumn(column);
    filterQuery.setId(id);
    filterQuery.setValue(Collections.singletonList(value));
    filterQuery.setOperator(operator);
    if (realtimeBrokerRequest.getFilterSubQueryMap() == null) {
      realtimeBrokerRequest.setFilterSubQueryMap(new FilterQueryMap());
    }
    realtimeBrokerRequest.getFilterSubQueryMap().putToFilterQueryMap(id, filterQuery);
    return filterQuery;
  }
}

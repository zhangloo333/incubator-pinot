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
package org.apache.pinot.broker.requesthandler;

import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.FilterQuery;
import org.apache.pinot.common.request.FilterQueryMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LowWaterMarkQueryWriter {
  private static final String VIRTUAL_COLUMN_PARTITION = "$partition";
  private static final String VALID_FROM = "$validFrom";
  private static final String VALID_UNTIL = "$validUntil";
  private static final int QUERY_ID_BASE = -1000;

  /**
   * For upsert enabled tables, augment the realtime query with low water mark constraints in its filter query.
   * @param realtimeBrokerRequest
   * @param lowWaterMarks
   */
  public static void addLowWaterMarkToQuery(BrokerRequest realtimeBrokerRequest, Map<Integer, Long> lowWaterMarks) {
    if (lowWaterMarks == null || lowWaterMarks.size() == 0) {
      return;
    }
    // 1. Build the low water mark query of the form for a table with partitions 0,1,..,n assuming -1 is used as
    // unitialized marker.
    // ($partition == 0 AND ($validFrom <= lwm_0 and $validFrom > -1) AND (lwm_0 < $validUtil OR $validUtil = -1))
    //  ... OR
    // ($partition == n AND ($ValidFrom <= lwm_n and $validFrom > -1) AND (lwm_n < $validUtil OR $validUtil = -1))
    FilterQuery lwmQuery = new FilterQuery();
    // Set an unique id range for the augmented query.
    int queryIdBase = QUERY_ID_BASE;
    lwmQuery.setId(queryIdBase--);
    lwmQuery.setOperator(FilterOperator.OR);
    List<Integer> subQids = new ArrayList<>();
    for (Map.Entry<Integer, Long> partitionLWM : lowWaterMarks.entrySet()) {
      FilterQuery singlePartitionQuery = addSinglePartitionLowWaterMark(queryIdBase, realtimeBrokerRequest,
          partitionLWM.getKey(), partitionLWM.getValue());
      queryIdBase = singlePartitionQuery.getId() - 1;
      subQids.add(singlePartitionQuery.getId());
    }
    lwmQuery.setNestedFilterQueryIds(subQids);

    // 2. Attach low water mark filter to the current filters.
    FilterQuery currentFilterQuery = realtimeBrokerRequest.getFilterQuery();
    if (currentFilterQuery != null) {
      FilterQuery andFilterQuery = new FilterQuery();
      andFilterQuery.setId(queryIdBase--);
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
   * @param partition
   * @param lwm low water mark.
   * @return a filter query corresponding to the low water mark constraint of a single partition. The general form is:
   *         Partition == n AND $ValidFrom <= lwm_n AND (lwm_n < $validUtil OR $validUtil = -1)
   */
  private static FilterQuery addSinglePartitionLowWaterMark(int queryIdBase, BrokerRequest realtimeBrokerRequest, int partition,
      Long lwm) {

    // Partition filter query: i.e., Partition == n;
    FilterQuery partitionFilterQuery = getLeafFilterQuery(VIRTUAL_COLUMN_PARTITION, queryIdBase--, String.valueOf(partition), FilterOperator.EQUALITY, realtimeBrokerRequest);
    // ValidFrom Query.
    FilterQuery validFromFilterQuery = new FilterQuery();
    validFromFilterQuery.setId(queryIdBase--);
    validFromFilterQuery.setOperator(FilterOperator.AND);
    FilterQuery validFromP1 = getLeafFilterQuery(VALID_FROM, queryIdBase--, "(*\t\t" + lwm + "]", FilterOperator.RANGE, realtimeBrokerRequest);
    FilterQuery validFromP2 = getLeafFilterQuery(VALID_FROM, queryIdBase--, "(-1\t\t*)", FilterOperator.RANGE, realtimeBrokerRequest);
    List<Integer> nestedQueriesIdForValidFrom = new ArrayList<>();
    nestedQueriesIdForValidFrom.add(validFromP1.getId());
    nestedQueriesIdForValidFrom.add(validFromP2.getId());
    validFromFilterQuery.setNestedFilterQueryIds(nestedQueriesIdForValidFrom);

    // ValidUtilQuery.
    FilterQuery validUtilFilterQuery = new FilterQuery();
    validUtilFilterQuery.setId(queryIdBase--);
    validUtilFilterQuery.setOperator(FilterOperator.OR);

    FilterQuery validUtilP1 = getLeafFilterQuery(VALID_UNTIL, queryIdBase--, "(" + lwm + "\t\t*)", FilterOperator.RANGE, realtimeBrokerRequest);
    FilterQuery validUtilP2 = getLeafFilterQuery(VALID_UNTIL, queryIdBase--, "-1", FilterOperator.EQUALITY, realtimeBrokerRequest);
    List<Integer> nestedQueriesIdForValidUtil = new ArrayList<>();
    nestedQueriesIdForValidUtil.add(validUtilP1.getId());
    nestedQueriesIdForValidUtil.add(validUtilP2.getId());
    validUtilFilterQuery.setNestedFilterQueryIds(nestedQueriesIdForValidUtil);

    // Top level query
    FilterQuery singlePartitionLWMQuery = new FilterQuery();
    singlePartitionLWMQuery.setId(queryIdBase--);
    singlePartitionLWMQuery.setOperator(FilterOperator.AND);
    List<Integer> nestQids = new ArrayList<>();
    nestQids.add(partitionFilterQuery.getId());
    nestQids.add(validFromFilterQuery.getId());
    nestQids.add(validUtilFilterQuery.getId());
    singlePartitionLWMQuery.setNestedFilterQueryIds(nestQids);

    // Add all the new created queries to the query map.
    realtimeBrokerRequest.getFilterSubQueryMap().putToFilterQueryMap(singlePartitionLWMQuery.getId(), singlePartitionLWMQuery);
    realtimeBrokerRequest.getFilterSubQueryMap().putToFilterQueryMap(validFromFilterQuery.getId(), validFromFilterQuery);
    realtimeBrokerRequest.getFilterSubQueryMap().putToFilterQueryMap(validUtilFilterQuery.getId(), validUtilFilterQuery);
    return singlePartitionLWMQuery;
  }

  private static FilterQuery getLeafFilterQuery(String column, int id, String value, FilterOperator operator,
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

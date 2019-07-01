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
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.thrift.TException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LowWaterMarkQueryWriterTest {
    @Test
    public void testRewriteQueryWithoutExistingFilters() {
        Pql2Compiler pql2Compiler = new Pql2Compiler();
        BrokerRequest req = pql2Compiler.compileToBrokerRequest("SELECT * FROM T");
        Assert.assertFalse(req.isSetFilterQuery());
        Map<Integer, Long> lwms = new HashMap<>();
        lwms.put(0, 10L);
        lwms.put(1, 20L);
        LowWaterMarkQueryWriter.addLowWaterMarkToQuery(req, lwms);
        Assert.assertTrue(req.isSetFilterQuery());
        try {
            req.validate();
        } catch (TException e)   {
            Assert.fail("Query after low water mark query is not valid: ", e);
        }
        // Verify there are in total 13 filter query nodes in the filter query tree.
        Map<Integer,FilterQuery> filterSubQueryMap = req.getFilterSubQueryMap().getFilterQueryMap();
        Assert.assertEquals(filterSubQueryMap.size(), 17);
        // 0. Verify there are one top level filter of operator OR with two sub filter queries.
        verifyNoneTerminalFilterQuery(req.getFilterQuery(), FilterOperator.OR, 2);

        // Verify the queries for both partitions
        Integer p1QueryId = req.getFilterQuery().getNestedFilterQueryIds().get(0);
        Integer p2QueryId = req.getFilterQuery().getNestedFilterQueryIds().get(1);
        // 1. Verify the first partition's filter query.
        FilterQuery partition1 = filterSubQueryMap.get(p1QueryId);
        verifyNoneTerminalFilterQuery(partition1, FilterOperator.AND, 3);
        FilterQuery partition1NumberQuery = filterSubQueryMap.get(partition1.getNestedFilterQueryIds().get(0));
        FilterQuery validFrom1Query = filterSubQueryMap.get(partition1.getNestedFilterQueryIds().get(1));
        FilterQuery validTo1Query = filterSubQueryMap.get(partition1.getNestedFilterQueryIds().get(2));

        verifyTerminalFilterQuery(partition1NumberQuery, "$partition", "0", FilterOperator.EQUALITY);
        // Verify the subtree (i.e., an AND with two nodes) for the $validFrom column.
        verifyNoneTerminalFilterQuery(validFrom1Query, FilterOperator.AND, 2);
        verifyTerminalFilterQuery(filterSubQueryMap.get(validFrom1Query.getNestedFilterQueryIds().get(0)),
            "$validFrom", "(*\t\t10]", FilterOperator.RANGE);
        verifyTerminalFilterQuery(filterSubQueryMap.get(validFrom1Query.getNestedFilterQueryIds().get(1)),
            "$validFrom", "(-1\t\t*)", FilterOperator.RANGE);

        // Verify the subtree (i.e., an OR with two nodes) for the $validutil column.
        verifyNoneTerminalFilterQuery(validTo1Query, FilterOperator.OR, 2);
        verifyTerminalFilterQuery(filterSubQueryMap.get(validTo1Query.getNestedFilterQueryIds().get(0)),
            "$validUntil", "(10\t\t*)", FilterOperator.RANGE);
        verifyTerminalFilterQuery(filterSubQueryMap.get(validTo1Query.getNestedFilterQueryIds().get(1)),
            "$validUntil", "-1", FilterOperator.EQUALITY);
        // 2. Verify the second partition's filter query.
        FilterQuery partition2 = filterSubQueryMap.get(p2QueryId);
        verifyNoneTerminalFilterQuery(partition2, FilterOperator.AND, 3);
        FilterQuery partition2NumberQuery = filterSubQueryMap.get(partition2.getNestedFilterQueryIds().get(0));
        FilterQuery validFrom2Query = filterSubQueryMap.get(partition2.getNestedFilterQueryIds().get(1));
        FilterQuery validTo2Query = filterSubQueryMap.get(partition2.getNestedFilterQueryIds().get(2));

        verifyTerminalFilterQuery(partition2NumberQuery, "$partition", "1", FilterOperator.EQUALITY);
        // Verify the subtree (i.e., an AND with two nodes) for the $validFrom column.
        verifyNoneTerminalFilterQuery(validFrom1Query, FilterOperator.AND, 2);
        verifyTerminalFilterQuery(filterSubQueryMap.get(validFrom2Query.getNestedFilterQueryIds().get(0)),
            "$validFrom", "(*\t\t20]", FilterOperator.RANGE);
        verifyTerminalFilterQuery(filterSubQueryMap.get(validFrom2Query.getNestedFilterQueryIds().get(1)),
            "$validFrom", "(-1\t\t*)", FilterOperator.RANGE);

        // Verify the subtree (i.e., an OR with two nodes) for the $validutil column.
        verifyNoneTerminalFilterQuery(validTo2Query, FilterOperator.OR, 2);
        verifyTerminalFilterQuery(filterSubQueryMap.get(validTo2Query.getNestedFilterQueryIds().get(0)),
            "$validUntil", "(20\t\t*)", FilterOperator.RANGE);
        verifyTerminalFilterQuery(filterSubQueryMap.get(validTo2Query.getNestedFilterQueryIds().get(1)),
            "$validUntil", "-1", FilterOperator.EQUALITY);
    }

    @Test
    public void testRewriteQueryWithExistingFilters() {
        Pql2Compiler pql2Compiler = new Pql2Compiler();
        BrokerRequest req = pql2Compiler.compileToBrokerRequest("SELECT * FROM T WHERE A < 4");
        Assert.assertTrue(req.isSetFilterQuery());
        Map<Integer, Long> lwms = new HashMap<>();
        lwms.put(0, 10L);
        lwms.put(1, 20L);
        LowWaterMarkQueryWriter.addLowWaterMarkToQuery(req, lwms);
        Assert.assertTrue(req.isSetFilterQuery());
        try {
            req.validate();
        } catch (TException e) {
            Assert.fail("Query after low water mark query is not valid: ", e);
        }
        // Verify there are in total 15 filter query nodes in the filter query tree.
        Map<Integer,FilterQuery> filterSubQueryMap = req.getFilterSubQueryMap().getFilterQueryMap();
        Assert.assertEquals(filterSubQueryMap.size(), 19);
        // 0. Verify there are one top level filter of operator OR with two sub filter queries.
        FilterQuery rootFilterQuery = req.getFilterQuery();
        verifyNoneTerminalFilterQuery(rootFilterQuery, FilterOperator.AND, 2);
        // 1. Verify the existing filter query is not affected.
        verifyTerminalFilterQuery(filterSubQueryMap.get(rootFilterQuery.getNestedFilterQueryIds().get(0)), "A", "(*\t\t4)", FilterOperator.RANGE);

        FilterQuery lowWaterMarkQuery = filterSubQueryMap.get(rootFilterQuery.getNestedFilterQueryIds().get(1));
        // Verify the queries for both partitions
        Integer p1QueryId = lowWaterMarkQuery.getNestedFilterQueryIds().get(0);
        Integer p2QueryId = lowWaterMarkQuery.getNestedFilterQueryIds().get(1);
        // 2. Verify the first partition's filter query.
        FilterQuery partition1 = filterSubQueryMap.get(p1QueryId);
        verifyNoneTerminalFilterQuery(partition1, FilterOperator.AND, 3);
        FilterQuery partition1NumberQuery = filterSubQueryMap.get(partition1.getNestedFilterQueryIds().get(0));
        FilterQuery validFrom1Query = filterSubQueryMap.get(partition1.getNestedFilterQueryIds().get(1));
        FilterQuery validTo1Query = filterSubQueryMap.get(partition1.getNestedFilterQueryIds().get(2));

        verifyTerminalFilterQuery(partition1NumberQuery, "$partition", "0", FilterOperator.EQUALITY);
        // Verify the subtree (i.e., an AND with two nodes) for the $validFrom column.
        verifyNoneTerminalFilterQuery(validFrom1Query, FilterOperator.AND, 2);
        verifyTerminalFilterQuery(filterSubQueryMap.get(validFrom1Query.getNestedFilterQueryIds().get(0)),
            "$validFrom", "(*\t\t10]", FilterOperator.RANGE);
        verifyTerminalFilterQuery(filterSubQueryMap.get(validFrom1Query.getNestedFilterQueryIds().get(1)),
            "$validFrom", "(-1\t\t*)", FilterOperator.RANGE);

        // Verify the subtree (i.e., an OR with two nodes) for the $validutil column.
        verifyNoneTerminalFilterQuery(validTo1Query, FilterOperator.OR, 2);
        verifyTerminalFilterQuery(filterSubQueryMap.get(validTo1Query.getNestedFilterQueryIds().get(0)),
            "$validUntil", "(10\t\t*)", FilterOperator.RANGE);
        verifyTerminalFilterQuery(filterSubQueryMap.get(validTo1Query.getNestedFilterQueryIds().get(1)),
            "$validUntil", "-1", FilterOperator.EQUALITY);
        // 3. Verify the second partition's filter query.
        FilterQuery partition2 = filterSubQueryMap.get(p2QueryId);
        verifyNoneTerminalFilterQuery(partition2, FilterOperator.AND, 3);
        FilterQuery partition2NumberQuery = filterSubQueryMap.get(partition2.getNestedFilterQueryIds().get(0));
        FilterQuery validFrom2Query = filterSubQueryMap.get(partition2.getNestedFilterQueryIds().get(1));
        FilterQuery validTo2Query = filterSubQueryMap.get(partition2.getNestedFilterQueryIds().get(2));

        verifyTerminalFilterQuery(partition2NumberQuery, "$partition", "1", FilterOperator.EQUALITY);
        // Verify the subtree (i.e., an AND with two nodes) for the $validFrom column.
        verifyNoneTerminalFilterQuery(validFrom1Query, FilterOperator.AND, 2);
        verifyTerminalFilterQuery(filterSubQueryMap.get(validFrom2Query.getNestedFilterQueryIds().get(0)),
            "$validFrom", "(*\t\t20]", FilterOperator.RANGE);
        verifyTerminalFilterQuery(filterSubQueryMap.get(validFrom2Query.getNestedFilterQueryIds().get(1)),
            "$validFrom", "(-1\t\t*)", FilterOperator.RANGE);

        // Verify the subtree (i.e., an OR with two nodes) for the $validutil column.
        verifyNoneTerminalFilterQuery(validTo2Query, FilterOperator.OR, 2);
        verifyTerminalFilterQuery(filterSubQueryMap.get(validTo2Query.getNestedFilterQueryIds().get(0)),
            "$validUntil", "(20\t\t*)", FilterOperator.RANGE);
        verifyTerminalFilterQuery(filterSubQueryMap.get(validTo2Query.getNestedFilterQueryIds().get(1)),
            "$validUntil", "-1", FilterOperator.EQUALITY);
    }

    private void verifyTerminalFilterQuery(FilterQuery filterQuery, String column, String value, FilterOperator op) {
        Assert.assertEquals(filterQuery.getColumn(), column);
        Assert.assertEquals(filterQuery.getValue(), Collections.singletonList(value));
        Assert.assertEquals(filterQuery.getOperator(), op);
    }

    private void verifyNoneTerminalFilterQuery(FilterQuery filterQuery, FilterOperator op, int numOfChildQueries) {
        Assert.assertEquals(filterQuery.getOperator(), op);
        Assert.assertEquals(filterQuery.getNestedFilterQueryIdsSize(), numOfChildQueries);
    }
}

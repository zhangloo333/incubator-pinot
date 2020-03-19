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

import org.apache.pinot.broker.requesthandler.LowWaterMarkQueryWriter;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.FilterQuery;
import org.apache.pinot.core.segment.updater.LowWaterMarkService;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.thrift.TException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class UpsertQueryRewriterImplTest {

    private LowWaterMarkService _lwms;
    private UpsertQueryRewriterImpl rewriter;

    @BeforeClass
    public void init() {
        _lwms = new PollingBasedLowWaterMarkService();
        rewriter = new UpsertQueryRewriterImpl(_lwms);
    }

    @Test
    public void testRewriteQueryWithoutExistingFilters() throws Exception{
        Pql2Compiler pql2Compiler = new Pql2Compiler();
        BrokerRequest req = pql2Compiler.compileToBrokerRequest("SELECT * FROM T");
        Assert.assertFalse(req.isSetFilterQuery());
        Map<Integer, Long> lwms = new HashMap<>();
        lwms.put(0, 10L);
        lwms.put(1, 20L);
        rewriter.addLowWaterMarkToQuery(req, lwms);
        Assert.assertTrue(req.isSetFilterQuery());
        try {
            req.validate();
        } catch (TException e)   {
            Assert.fail("Query after low water mark query is not valid: ", e);
        }
        // Verify there are in total 7 filter query nodes in the filter query tree.
        Map<Integer,FilterQuery> filterSubQueryMap = req.getFilterSubQueryMap().getFilterQueryMap();
        Assert.assertEquals(filterSubQueryMap.size(), 7);

        Integer lwmQueryId = req.getFilterQuery().getId();
        // 1. Verify the low water mark query.
        FilterQuery lwmQuery = filterSubQueryMap.get(lwmQueryId);
        verifyNoneTerminalFilterQuery(lwmQuery, FilterOperator.AND, 2);
        FilterQuery validFrom1Query = filterSubQueryMap.get(lwmQuery.getNestedFilterQueryIds().get(0));
        FilterQuery validTo1Query = filterSubQueryMap.get(lwmQuery.getNestedFilterQueryIds().get(1));

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
    }

    @Test
    public void testRewriteQueryWithExistingFilters() {
        Pql2Compiler pql2Compiler = new Pql2Compiler();
        BrokerRequest req = pql2Compiler.compileToBrokerRequest("SELECT * FROM T WHERE A < 4");
        Assert.assertTrue(req.isSetFilterQuery());
        Map<Integer, Long> lwms = new HashMap<>();
        lwms.put(0, 10L);
        lwms.put(1, 20L);
        rewriter.addLowWaterMarkToQuery(req, lwms);
        Assert.assertTrue(req.isSetFilterQuery());
        try {
            req.validate();
        } catch (TException e) {
            Assert.fail("Query after low water mark query is not valid: ", e);
        }
        // Verify there are in total 9 filter query nodes in the filter query tree.
        Map<Integer,FilterQuery> filterSubQueryMap = req.getFilterSubQueryMap().getFilterQueryMap();
        Assert.assertEquals(filterSubQueryMap.size(), 9);
        // 0. Verify there are one top level filter of operator OR with two sub filter queries.
        FilterQuery rootFilterQuery = req.getFilterQuery();
        verifyNoneTerminalFilterQuery(rootFilterQuery, FilterOperator.AND, 2);
        // 1. Verify the existing filter query A < 4 is not affected.
        verifyTerminalFilterQuery(filterSubQueryMap.get(rootFilterQuery.getNestedFilterQueryIds().get(0)), "A", "(*\t\t4)", FilterOperator.RANGE);

        FilterQuery lowWaterMarkQuery = filterSubQueryMap.get(rootFilterQuery.getNestedFilterQueryIds().get(1));
        // Verify the lwm query
        verifyNoneTerminalFilterQuery(lowWaterMarkQuery, FilterOperator.AND, 2);
        FilterQuery validFrom1Query = filterSubQueryMap.get(lowWaterMarkQuery.getNestedFilterQueryIds().get(0));
        FilterQuery validTo1Query = filterSubQueryMap.get(lowWaterMarkQuery.getNestedFilterQueryIds().get(1));

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

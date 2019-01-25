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
package org.apache.pinot.core.operator.filter;

import com.google.common.base.Preconditions;
import org.apache.lucene.search.TopDocs;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.common.predicate.MatchesPredicate;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.filter.predicate.MatchesPredicateEvaluatorFactory.DefaultMatchesPredicateEvaluator;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexBasedMatchesFilterOperator extends BaseFilterOperator {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IndexBasedMatchesFilterOperator.class);
  private static final String OPERATOR_NAME = "IndexBasedMatchesFilterOperator";

  private final DataSource _dataSource;
  private final int _startDocId;
  // TODO: change it to exclusive
  // Inclusive
  private final int _endDocId;
  private MatchesPredicate _matchesPredicate;

  public IndexBasedMatchesFilterOperator(PredicateEvaluator predicateEvaluator,
      DataSource dataSource, int startDocId, int endDocId) {
    // NOTE:
    // Predicate that is always evaluated as true or false should not be passed into the
    // TextMatchFilterOperator for
    // performance concern.
    // If predicate is always evaluated as true, use MatchAllFilterOperator; if predicate is always
    // evaluated as false,
    // use EmptyFilterOperator.
    Preconditions
        .checkArgument(!predicateEvaluator.isAlwaysTrue() && !predicateEvaluator.isAlwaysFalse());
    Preconditions.checkArgument(predicateEvaluator instanceof DefaultMatchesPredicateEvaluator);

    DefaultMatchesPredicateEvaluator evaluator =
        (DefaultMatchesPredicateEvaluator) predicateEvaluator;
    _matchesPredicate = evaluator.getMatchesPredicate();
    _dataSource = dataSource;
    _startDocId = startDocId;
    _endDocId = endDocId;
  }

  @Override
  protected FilterBlock getNextBlock() {

    InvertedIndexReader invertedIndex = _dataSource.getInvertedIndex();
    MutableRoaringBitmap bitmap = (MutableRoaringBitmap) invertedIndex.getDocIds(_matchesPredicate);

    boolean exclusive = false;
    ImmutableRoaringBitmap[] bitmapArray = new ImmutableRoaringBitmap[] {
        bitmap
    };
    return new FilterBlock(new BitmapDocIdSet(bitmapArray, _startDocId, _endDocId, exclusive));
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}

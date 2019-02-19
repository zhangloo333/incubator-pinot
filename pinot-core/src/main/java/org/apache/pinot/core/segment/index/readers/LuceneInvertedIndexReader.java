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
package org.apache.pinot.core.segment.index.readers;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.surround.query.FieldsQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.pinot.common.data.FieldSpec.DataType;
import org.apache.pinot.common.data.FieldSpec.FieldType;
import org.apache.pinot.common.data.objects.TextObject;
import org.apache.pinot.core.common.Predicate;
import org.apache.pinot.core.common.predicate.MatchesPredicate;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.core.segment.index.ColumnMetadata;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class LuceneInvertedIndexReader implements InvertedIndexReader<MutableRoaringBitmap> {

  private static final Logger LOGGER = LoggerFactory.getLogger(LuceneInvertedIndexReader.class);
  private final IndexSearcher _searcher;
  private final Analyzer _analyzer = new PerFieldAnalyzerWrapper(new StandardAnalyzer());
  private Directory _index;

  /**
   * TODO: Change this to take PinotDataBuffer, work around for now since Lucene needs actual
   * directory
   * @param segmentIndexDir
   * @param metadata
   */
  public LuceneInvertedIndexReader(File segmentIndexDir, ColumnMetadata metadata) {

    try {
      File searchIndexDir = new File(segmentIndexDir.getPath(),
          metadata.getColumnName() + V1Constants.Indexes.LUCENE_INVERTED_INDEX_DIR);
      _index = FSDirectory.open(searchIndexDir.toPath());
      IndexReader reader = DirectoryReader.open(_index);
      _searcher = new IndexSearcher(reader);
    } catch (IOException e) {
      LOGGER.error("Encountered error creating LuceneSearchIndexReader ", e);
      throw new RuntimeException(e);
    }
  }

  public LuceneInvertedIndexReader(IndexReader reader) {
    _searcher = new IndexSearcher(reader);
  }

  @Override
  public void close() throws IOException {
    _index.close();
  }

  @Override
  public MutableRoaringBitmap getDocIds(int dictId) {
    throw new UnsupportedOperationException(
        "DictId based evaluation not supported for Lucene based Indexing scheme");
  }

  @Override
  public MutableRoaringBitmap getDocIds(Predicate predicate) {
    MatchesPredicate matchesPredicate = (MatchesPredicate) predicate;
    MutableRoaringBitmap bitmap =
        getDocIds(matchesPredicate.getQuery(), matchesPredicate.getQueryOptions());
    return bitmap;
  }

  public MutableRoaringBitmap getDocIds(String queryStr, String options) {
    QueryParser queryParser = new QueryParser(TextObject.DEFAULT_FIELD, _analyzer);
    Query query;
    try {
      query = queryParser.parse(queryStr);
    } catch (ParseException e) {
      LOGGER.error("Encountered exception while parsing query {}", queryStr, e);
      throw new RuntimeException(e);
    }
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    try {
      Collector collector = createCollector(bitmap);
      _searcher.search(query, collector);
    } catch (IOException e) {
      LOGGER.error("Encountered exception while executing search query {}", queryStr, e);
      throw new RuntimeException(e);
    }
    return bitmap;
  }

  private Collector createCollector(final MutableRoaringBitmap bitmap) {
    return new LuceneResultCollector(bitmap);
  }

  public static final class LuceneResultCollector implements Collector {
    private final MutableRoaringBitmap bitmap;

    public LuceneResultCollector(MutableRoaringBitmap bitmap) {
      this.bitmap = bitmap;
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      return new LeafCollector() {

        @Override
        public void setScorer(Scorer scorer) throws IOException {
          // ignore
        }

        @Override
        public void collect(int doc) throws IOException {
          bitmap.add(doc);
        }
      };
    }
  }
}

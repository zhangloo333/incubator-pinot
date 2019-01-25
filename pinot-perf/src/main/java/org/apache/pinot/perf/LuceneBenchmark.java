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
package org.apache.pinot.perf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.pinot.core.segment.index.readers.LuceneInvertedIndexReader;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

public class LuceneBenchmark {

  public static void main(String[] args) throws IOException, ParseException {
    Map<String, Analyzer> analyzerMap = new HashMap<>();
    Analyzer wrapper = new PerFieldAnalyzerWrapper(new StandardAnalyzer());
    Directory index = new RAMDirectory();
    IndexWriterConfig config = new IndexWriterConfig(wrapper);

    try (IndexWriter writer = new IndexWriter(index, config)) {
      for (int i = 0; i < 1000; i++) {
        Document doc = new Document();
        doc.add(new TextField("k1", "value-" + i, Field.Store.YES));
        doc.add(new TextField("k2", "value-" + i, Field.Store.YES));
        writer.addDocument(doc);
      }
    }
    String querystr;
    querystr = "k1:\"value1?0\"";
    Query q = new QueryParser("Content", wrapper).parse(querystr);
    q = new WildcardQuery(new Term("k1", QueryParser.escape("value1*")));

    // 3. searching
    IndexReader reader = DirectoryReader.open(index);
    IndexSearcher searcher = new IndexSearcher(reader);
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    Collector collector = new LuceneInvertedIndexReader.LuceneResultCollector(bitmap);
    searcher.search(q, collector);

    // 4. display results
    System.out.println("Query string: " + querystr);
    System.out.println("Found " + bitmap.getCardinality() + " hits.");

  }
}

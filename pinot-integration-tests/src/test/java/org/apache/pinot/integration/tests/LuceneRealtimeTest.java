package org.apache.pinot.integration.tests;

import java.io.File;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.FSDirectory;
import org.apache.pinot.core.segment.index.readers.LuceneInvertedIndexReader;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class LuceneRealtimeTest {

  private static Analyzer _analyzer;
  private static IndexWriterConfig _indexWriterConfig;
  private static FSDirectory _indexDirectory;
  private static IndexWriter _writer;
//  private static IndexSearcher _searcher;

  public static void main(String[] args)
      throws Exception {

    _analyzer = new PerFieldAnalyzerWrapper(new StandardAnalyzer());

    _indexWriterConfig = new IndexWriterConfig(_analyzer);
    _indexWriterConfig.setRAMBufferSizeMB(500);
    String indexDir = "/tmp/lucene-realtime-test-" + System.currentTimeMillis();
    File outputDirectory = new File(indexDir);
    outputDirectory.delete();
    outputDirectory.mkdirs();
    _indexDirectory = FSDirectory.open(outputDirectory.toPath());
    _writer = new IndexWriter(_indexDirectory, _indexWriterConfig);
//    DirectoryReader reader = DirectoryReader.open(_writer);
//    _searcher = new IndexSearcher(reader);
    SearcherManager manager;
    manager = new SearcherManager(_writer, null);
    for (int i = 0; i < 100; i++) {
      Document document = new Document();
      TextField field = new TextField("k1", "value" + i, Field.Store.YES);
      document.add(field);
      _writer.addDocument(document);
      _writer.maybeMerge();
      _writer.commit();
      //FIXME: Uncommenting this line breaks everything. It some how messes up the indexWriter
      query(_writer, manager, "k1:value" + i, i);
    }
    _writer.commit();

    for (int i = 0; i < 100; i++) {
      query(_writer, manager, "k1:value" + i, i);
    }

    _writer.close();
    DirectoryReader reader = DirectoryReader.open(_indexDirectory);
    manager = new SearcherManager(reader, null);
    for (int i = 0; i < 100; i++) {
      query(_writer, manager, "k1:value" + i, i);
    }

  }

  private static void query(IndexWriter writer, SearcherManager manager, String queryStr, int i)
      throws ParseException, java.io.IOException {
//    manager.maybeRefresh();
//    IndexSearcher searcher = manager.acquire();

    QueryParser queryParser = new QueryParser("k1", _analyzer);
    Query query = queryParser.parse(queryStr);
//    query = new TermQuery(new Term("k1", "value"+ i ));
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    LuceneInvertedIndexReader.LuceneResultCollector resultCollector =
        new LuceneInvertedIndexReader.LuceneResultCollector(bitmap);
    IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(_writer));
    searcher.search(query, resultCollector);
    System.out.println("queryStr=" + query + " bitmap = " + bitmap);
//    manager.release(searcher);
  }
}

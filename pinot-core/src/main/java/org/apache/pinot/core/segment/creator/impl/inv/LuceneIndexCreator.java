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
package org.apache.pinot.core.segment.creator.impl.inv;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.PinotObject;
import org.apache.pinot.core.segment.creator.InvertedIndexCreator;
import org.apache.pinot.core.segment.index.ColumnMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LuceneIndexCreator implements InvertedIndexCreator {

  private static final Logger LOGGER = LoggerFactory.getLogger(InvertedIndexCreator.class);

  public static final String VERSION = "7.6.0";
  // Index file will be flushed after reaching this threshold
  private static final int MAX_BUFFER_SIZE_MB = 500;
  private static final Field.Store DEFAULT_STORE = Field.Store.NO;
  private final Analyzer _analyzer;
  private final IndexWriter _writer;
  private final IndexWriterConfig _indexWriterConfig;
  private final Directory _indexDirectory;
  // TODO:Figure out a way to avoid this
  boolean _isText = false;

  public LuceneIndexCreator(ColumnMetadata columnMetadata, File outputDirectory) {
    // TODO: Get IndexConfig and set the different analyzer for each field by default we set
    // StandardAnalyzer and use TextField. This can be expensive and inefficient if all we need is
    // exact match. See keyword analyzer
    _analyzer = new PerFieldAnalyzerWrapper(new StandardAnalyzer());
    _indexWriterConfig = new IndexWriterConfig(_analyzer);
    _indexWriterConfig.setRAMBufferSizeMB(MAX_BUFFER_SIZE_MB);
    _isText = "TEXT".equalsIgnoreCase(columnMetadata.getObjectType());
    try {
      _indexDirectory = FSDirectory.open(outputDirectory.toPath());
      _writer = new IndexWriter(_indexDirectory, _indexWriterConfig);
    } catch (IOException e) {
      LOGGER.error("Encountered error creating LuceneIndexCreator ", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void add(int dictId) {
    throw new UnsupportedOperationException(
        "Lucene indexing not supported for dictionary encoded columns");
  }

  @Override
  public void add(int[] dictIds, int length) {
    throw new UnsupportedOperationException(
        "Lucene indexing not supported for dictionary encoded columns");

  }

  @Override
  public void add(PinotObject object) {
    Document document = new Document();
    List<String> propertyNames = object.getPropertyNames();
    for (String propertyName : propertyNames) {
      Field field;
      // TODO: Figure out a way to avoid special casing Text, have a way to get propertyType from
      // pinotObject?
      // TODO: Handle list field
      Object value = object.getProperty(propertyName);
      if (value.getClass().isAssignableFrom(List.class)) {
        List<?> list = (List<?>) value;
        for (Object item : list) {
          field = new TextField(propertyName, item.toString(), DEFAULT_STORE);
          document.add(field);
        }
      } else {
        field = new TextField(propertyName, value.toString(), DEFAULT_STORE);
        document.add(field);
      }
    }
    try {
      _writer.addDocument(document);
    } catch (IOException e) {
      LOGGER.error("Encountered exception while adding doc:{}", document.toString(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void seal() throws IOException {

  }

  @Override
  public void close() throws IOException {
    _writer.close();
  }

}

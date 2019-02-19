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
package org.apache.pinot.core.realtime.impl.invertedindex;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.lucene.index.DirectoryReader;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.PinotObject;
import org.apache.pinot.core.common.Predicate;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.core.segment.creator.impl.inv.LuceneIndexCreator;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;
import org.apache.pinot.core.segment.index.readers.LuceneInvertedIndexReader;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RealtimeInvertedIndexReader implements InvertedIndexReader<MutableRoaringBitmap> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeInvertedIndexReader.class);

  private final List<ThreadSafeMutableRoaringBitmap> _bitmaps = new ArrayList<>();
  private final ReentrantReadWriteLock.ReadLock _readLock;
  private final ReentrantReadWriteLock.WriteLock _writeLock;
  private LuceneInvertedIndexReader _reader;
  private LuceneIndexCreator _creator;
  boolean isLuceneInitialized;

  public RealtimeInvertedIndexReader(FieldSpec spec, String indexDir) {
    ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    _readLock = readWriteLock.readLock();
    _writeLock = readWriteLock.writeLock();
    if (spec.getObjectType() != null) {
      try {
        File outputDirectory = new File(indexDir, spec.getName() + V1Constants.Indexes.LUCENE_INVERTED_INDEX_DIR);
        _creator = new LuceneIndexCreator(spec.getObjectType(), outputDirectory);
        _reader = new LuceneInvertedIndexReader(DirectoryReader.open(_creator.getIndexDirectory()));
        LOGGER.info("Initializing Lucene for column:{}", spec.getName());
        isLuceneInitialized = true;
      } catch (IOException e) {
        LOGGER.error("Error initializing Lucene for column:{}", spec.getName(), e);
      }
    }
  }

  /**
   * Add the document id to the bitmap for the given dictionary id.
   */
  public void add(int docId, PinotObject pinotObject) {
    _creator.add(pinotObject);
  }

  /**
   * Add the document id to the bitmap for the given dictionary id.
   */
  public void add(int dictId, int docId) {
    if (_bitmaps.size() == dictId) {
      // Bitmap for the dictionary id does not exist, add a new bitmap into the list
      ThreadSafeMutableRoaringBitmap bitmap = new ThreadSafeMutableRoaringBitmap(docId);
      try {
        _writeLock.lock();
        _bitmaps.add(bitmap);
      } finally {
        _writeLock.unlock();
      }
    } else {
      // Bitmap for the dictionary id already exists, check and add document id into the bitmap
      _bitmaps.get(dictId).checkAndAdd(docId);
    }
  }

  @Override
  public MutableRoaringBitmap getDocIds(Predicate predicate) {
    return _reader.getDocIds(predicate);
  }

  @Override
  public MutableRoaringBitmap getDocIds(int dictId) {
    ThreadSafeMutableRoaringBitmap bitmap;
    try {
      _readLock.lock();
      bitmap = _bitmaps.get(dictId);
    } finally {
      _readLock.unlock();
    }
    return bitmap.getMutableRoaringBitmap();
  }

  @Override
  public void close() {
  }

  /**
   * Helper wrapper class for {@link MutableRoaringBitmap} to make it thread-safe.
   */
  private static class ThreadSafeMutableRoaringBitmap {
    private MutableRoaringBitmap _mutableRoaringBitmap;

    public ThreadSafeMutableRoaringBitmap(int firstDocId) {
      _mutableRoaringBitmap = new MutableRoaringBitmap();
      _mutableRoaringBitmap.add(firstDocId);
    }

    public void checkAndAdd(int docId) {
      if (!_mutableRoaringBitmap.contains(docId)) {
        synchronized (this) {
          _mutableRoaringBitmap.add(docId);
        }
      }
    }

    public synchronized MutableRoaringBitmap getMutableRoaringBitmap() {
      return _mutableRoaringBitmap.clone();
    }
  }
}

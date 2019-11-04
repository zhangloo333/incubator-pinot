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
package org.apache.pinot.grigio.common.keyValueStore;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RocksDBBatchReader implements Callable<Boolean> {
  static final int MAX_RETRY_ATTEMPTS = 3;
  static final long RETRY_WAIT_MS = 1000L;

  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBBatchReader.class);

  private final RocksDB _rocksDB;
  private final List<byte[]> _byteKeys;
  private Map<byte[], byte[]> _result;

  RocksDBBatchReader(RocksDB rocksDB, List<byte[]> byteKeys) {
    _rocksDB = rocksDB;
    _byteKeys = byteKeys;
  }

  Map<byte[], byte[]> getResult() {
    if (_result == null) {
      throw new RuntimeException("No data got from RocksDB yet!");
    }
    return _result;
  }

  @Override
  public Boolean call() {
    try {
      _result = _rocksDB.multiGet(_byteKeys);
      return true;
    } catch (RocksDBException e) {
      LOGGER.warn("Failed to read from RocksDB: ", e);
      return false;
    }
  }
}

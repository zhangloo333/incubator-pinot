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
package org.apache.pinot.grigio.common.storageProvider;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class UpdateLogStorageProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateLogStorageProvider.class);

  private final Configuration _conf;
  private final File _virtualColumnStorageDir;
  private final Map<String, Map<String, SegmentUpdateLogStorageProvider>> _virtualColumnStorage = new ConcurrentHashMap<>();

  public static final String BASE_PATH_CONF_KEY = "basePath";

  @VisibleForTesting
  protected static volatile UpdateLogStorageProvider _instance = null;

  public static synchronized void init(Configuration conf) {
    LOGGER.info("initializing virtual column storage");
    if (_instance == null) {
      _instance = new UpdateLogStorageProvider(conf);
    } else {
      throw new RuntimeException("validFrom storage has already been inited");
    }
  }

  public static UpdateLogStorageProvider getInstance() {
    if (_instance == null) {
      throw new RuntimeException("virtual column storage has not been inited");
    }
    return _instance;
  }

  private UpdateLogStorageProvider(Configuration conf) {
    _conf = conf;
    final String basePath = conf.getString(BASE_PATH_CONF_KEY);
    LOGGER.info("use base path {} for virtual column storage", basePath);
    if (StringUtils.isEmpty(basePath)) {
      throw new IllegalStateException("base path doesn't exists in config");
    }
    _virtualColumnStorageDir = new File(basePath);
    if (!_virtualColumnStorageDir.exists()) {
      LOGGER.info("virtual column storage path {} doesn't exist, creating now", basePath);
      _virtualColumnStorageDir.mkdirs();
    }
  }

  public synchronized void addSegment(String tableName, String segmentName) throws IOException {
    final File tableDir = new File(_virtualColumnStorageDir, tableName);
    if (!_virtualColumnStorage.containsKey(tableName)) {
      LOGGER.info("adding virtual column for table {}", tableName);
      if (!tableDir.exists()) {
        boolean result = tableDir.mkdir();
        Preconditions.checkState(result, "creating table path failed " + tableDir);
      }
      Preconditions.checkState(tableDir.isDirectory(), "table path is not directory " + tableDir);
    }
    Map<String, SegmentUpdateLogStorageProvider> segmentMap = _virtualColumnStorage.computeIfAbsent(tableName, t -> new ConcurrentHashMap<>());
    if (!segmentMap.containsKey(segmentName)) {
      LOGGER.info("adding virtual column for table {} segment {}", tableName, segmentName);
      final File segmentUpdateFile = new File(tableDir, segmentName);
      if (!segmentUpdateFile.exists()) {
        boolean result = segmentUpdateFile.createNewFile();
        Preconditions.checkState(result, "creating segment path failed " + tableDir);
      }
      Preconditions.checkState(segmentUpdateFile.isFile(), "expect segment log location as file");
      segmentMap.put(segmentName, new SegmentUpdateLogStorageProvider(segmentUpdateFile));
    }
  }

  public List<UpdateLogEntry> getAllMessages(String tableName, String segmentName) throws IOException {
    LOGGER.info("loading all message for table {} segment {}", tableName, segmentName);
    if (_virtualColumnStorage.containsKey(tableName)) {
      SegmentUpdateLogStorageProvider provider = _virtualColumnStorage.get(tableName).get(segmentName);
      if (provider != null) {
        return provider.readAllMessagesFromFile();
      } else {
        LOGGER.warn("don't have data for segment {}", segmentName);
        return ImmutableList.of();
      }
    } else {
      LOGGER.error("don't have data for table {}", tableName);
      return ImmutableList.of();
    }
  }
  public void addDataToFile(String tableName, String segmentName, List<UpdateLogEntry> messages) throws IOException {
    if (_virtualColumnStorage.containsKey(tableName)) {
      Map<String, SegmentUpdateLogStorageProvider> segmentProviderMap =  _virtualColumnStorage.get(tableName);
      if (!segmentProviderMap.containsKey(segmentName)) {
        // TODO fix this part as we are adding all segment metadata
        // need to work on new design to prevent writing too much data
        addSegment(tableName, segmentName);
      }
      segmentProviderMap.get(segmentName).addData(messages);
    } else {
      LOGGER.warn("receive update event for table {} not in this server", tableName);
    }
  }

  public synchronized void removeSegment(String tableName, String segmentName) throws IOException {
    if (_virtualColumnStorage.containsKey(tableName)) {
      SegmentUpdateLogStorageProvider provider = _virtualColumnStorage.get(tableName).remove(segmentName);
      if (provider != null) {
        LOGGER.info("deleting table {} segment {}", tableName, segmentName);
        provider.destroy();
      }
    }
  }
}

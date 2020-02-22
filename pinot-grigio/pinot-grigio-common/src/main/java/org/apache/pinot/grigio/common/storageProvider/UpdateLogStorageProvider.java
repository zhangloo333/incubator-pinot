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
import com.google.common.collect.ImmutableSet;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class UpdateLogStorageProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateLogStorageProvider.class);

  private final Configuration _conf;
  private final File _virtualColumnStorageDir;
  private final Map<String, Map<String, SegmentUpdateLogStorageProvider>> _virtualColumnStorage = new ConcurrentHashMap<>();
  private volatile boolean _isClosed = false;

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
    maybeAddTableToMetadata(tableName);
    Map<String, SegmentUpdateLogStorageProvider> segmentMap = _virtualColumnStorage.get(tableName);
    if (!segmentMap.containsKey(segmentName)) {
      File tableDir = new File(_virtualColumnStorageDir, tableName);
      LOGGER.info("adding local update log storage for table {} segment {}", tableName, segmentName);
      final File segmentUpdateFile = new File(tableDir, segmentName);
      if (!segmentUpdateFile.exists()) {
        LOGGER.info("creating new local update log storage at {}", segmentUpdateFile.getPath());
        boolean result = segmentUpdateFile.createNewFile();
        Preconditions.checkState(result, "creating segment path failed " + tableDir);
      }
      Preconditions.checkState(segmentUpdateFile.isFile(), "expect segment log location as file");
      segmentMap.put(segmentName, new SegmentUpdateLogStorageProvider(segmentUpdateFile));
    }
  }

  /**
   * load all segment update logs under a table name in this update log provider
   * @param tableName the name of the table with type info
   * @throws IOException
   */
  public synchronized void loadTable(String tableName) throws IOException {
    LOGGER.info("loading table {}", tableName);
    final File tableDir = new File(_virtualColumnStorageDir, tableName);
    if (!tableDir.exists()) {
      LOGGER.warn("table directory does not exist at path {}", tableDir.getPath());
    } else {
      Map<String, SegmentUpdateLogStorageProvider> tableUpdateLogs = new ConcurrentHashMap<>();
      _virtualColumnStorage.put(tableName, tableUpdateLogs);
      File[] segmentFiles = tableDir.listFiles();
      if (segmentFiles != null) {
        for (File segmentFile: segmentFiles) {
          tableUpdateLogs.put(segmentFile.getName(), new SegmentUpdateLogStorageProvider(segmentFile));
        }
        LOGGER.info("loaded {} segment from table", segmentFiles.length);
      }
    }
  }

  public synchronized Set<String> getAllSegments(String tableName) {
    if (_virtualColumnStorage.containsKey(tableName)) {
      return ImmutableSet.copyOf(_virtualColumnStorage.get(tableName).keySet());
    } else {
      return ImmutableSet.of();
    }
  }

  /**
   * add a table to internal mapping and ensure the local directory exists
   * @param tableName the name of the table we are adding
   */
  private synchronized void maybeAddTableToMetadata(String tableName) {
    final File tableDir = new File(_virtualColumnStorageDir, tableName);
    if (!_virtualColumnStorage.containsKey(tableName)) {
      LOGGER.info("adding virtual column for table {}", tableName);
      if (!tableDir.exists()) {
        boolean result = tableDir.mkdir();
        Preconditions.checkState(result, "creating table path failed " + tableDir);
      }
      Preconditions.checkState(tableDir.isDirectory(), "table path is not directory " + tableDir);
      _virtualColumnStorage.computeIfAbsent(tableName, t -> new ConcurrentHashMap<>());
    }
  }

  public UpdateLogEntrySet getAllMessages(String tableName, String segmentName) throws IOException {
    if (_virtualColumnStorage.containsKey(tableName)) {
      SegmentUpdateLogStorageProvider provider = _virtualColumnStorage.get(tableName).get(segmentName);
      if (provider != null) {
        return provider.readAllMessagesFromFile();
      } else {
        LOGGER.warn("don't have data for segment {}", segmentName);
        return UpdateLogEntrySet.getEmptySet();
      }
    } else {
      LOGGER.error("don't have data for table {}", tableName);
      return UpdateLogEntrySet.getEmptySet();
    }
  }

  public void addDataToFile(String tableName, String segmentName, List<UpdateLogEntry> messages) throws IOException {
    Preconditions.checkState(!_isClosed, "update log provider has been closed");
    maybeAddTableToMetadata(tableName);
    Map<String, SegmentUpdateLogStorageProvider> segmentProviderMap =  _virtualColumnStorage.get(tableName);
    if (!segmentProviderMap.containsKey(segmentName)) {
      // TODO fix this part as we are adding all segment metadata
      // need to work on new design to prevent writing too much data
      addSegment(tableName, segmentName);
    }
    segmentProviderMap.get(segmentName).addData(messages);
  }

  public synchronized void removeSegment(String tableName, String segmentName) throws IOException {
    if (_virtualColumnStorage.containsKey(tableName)) {
      SegmentUpdateLogStorageProvider provider = _virtualColumnStorage.get(tableName).remove(segmentName);
      if (provider != null) {
        LOGGER.info("deleting update log for table {} segment {}", tableName, segmentName);
        provider.destroy();
      } else {
        // will also try to delete update log file that are on this server but not loaded due to lazy-loading
        File segmentUpdateLogFile = new File(new File(_virtualColumnStorageDir, tableName), segmentName);
        if (segmentUpdateLogFile.exists()) {
          LOGGER.info("deleting old updates log for table {} segment {}", tableName, segmentName);
          segmentUpdateLogFile.delete();
        } else {
          LOGGER.info("trying to delete table {} segment {} but it doesn't exist", tableName, segmentName);
        }
      }
    } else {
      LOGGER.info("trying to delete table {} segment {} but table is not in the current server", tableName, segmentName);
    }
  }

  public synchronized void removeAllUpdateLogsForTable(String tableName) {
    LOGGER.info("removing all update log storage for the given table {}", tableName);
    if (_virtualColumnStorage.containsKey(tableName)) {
      for (String segmentName : _virtualColumnStorage.get(tableName).keySet()) {
        try {
          removeSegment(tableName, segmentName);
        } catch (IOException ex) {
          LOGGER.error("failed to remove segment {}:{}", tableName, segmentName);
        }
      }
      _virtualColumnStorage.remove(tableName);
    }
    final File tableDir = new File(_virtualColumnStorageDir, tableName);
    if (tableDir.exists() && tableDir.isDirectory()) {
      File[] segmentUpdateLogs = tableDir.listFiles();
      LOGGER.info("remove {} files under table directory {}", segmentUpdateLogs.length, tableDir.getAbsolutePath());
      for (File segmentUpdateLog : segmentUpdateLogs) {
        segmentUpdateLog.delete();
      }
    }
  }

  public synchronized void close() throws IOException {
    _isClosed = true;
    for (Map<String, SegmentUpdateLogStorageProvider> segmentUpdateLogStorageProviderMap: _virtualColumnStorage.values()) {
      for (SegmentUpdateLogStorageProvider provider: segmentUpdateLogStorageProviderMap.values()) {
        provider.close();
      }
    }
  }
}

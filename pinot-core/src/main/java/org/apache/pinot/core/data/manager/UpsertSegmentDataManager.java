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
package org.apache.pinot.core.data.manager;

import org.apache.pinot.grigio.common.storageProvider.UpdateLogEntry;

import java.util.List;

public interface UpsertSegmentDataManager {

  /**
   * update the upsert-related virtual columns with the new values in this list of update logs
   * @param messages list of updates logs to update the virtual columns
   */
  void updateVirtualColumns(List<UpdateLogEntry> messages);

  /**
   * get the upsert related virtual column debug info given an offset
   * @param offset the offset we want to look up the virtual column info from
   * @return debug info describing the upsert-related virtual column info
   */
  String getVirtualColumnInfo(long offset);
}

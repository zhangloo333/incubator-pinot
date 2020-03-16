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
package org.apache.pinot.core.data.manager.upsert;

import org.apache.pinot.grigio.common.storageProvider.UpdateLogEntry;
import org.apache.pinot.spi.data.readers.GenericRow;

import java.io.IOException;
import java.util.List;

public class DefaultDataManagerCallbackImpl implements DataManagerCallback {

  // expect to use the cached instance  for append mode to reduce memory usage
  public static final DefaultDataManagerCallbackImpl INSTANCE = new DefaultDataManagerCallbackImpl();

  private final IndexSegmentCallback _indexSegmentCallback;

  private DefaultDataManagerCallbackImpl() {
    _indexSegmentCallback = DefaultIndexSegmentCallback.INSTANCE;
  }

  public IndexSegmentCallback getIndexSegmentCallback() {
    return _indexSegmentCallback;
  }

  @Override
  public void processTransformedRow(GenericRow row, long offset) {
    // do nothing
  }

  @Override
  public void postIndexProcessing(GenericRow row, long offset) {
    // do nothing
  }

  @Override
  public void postConsumeLoop() {
    // do nothing
  }

  @Override
  public void initVirtualColumns() throws IOException {
    // do nothing
  }

  @Override
  public void updateVirtualColumns(List<UpdateLogEntry> messages) {
    // do nothing
  }

  @Override
  public void destroy() {
    // do nothing
  }
}

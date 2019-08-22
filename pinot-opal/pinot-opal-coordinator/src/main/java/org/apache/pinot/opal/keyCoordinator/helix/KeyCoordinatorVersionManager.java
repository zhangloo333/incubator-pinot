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
package org.apache.pinot.opal.keyCoordinator.helix;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manager for version numbers. This handles getting/setting version produced from/to zookeeper
 * property store. Utilizes cache built in ZkHelixPropertyStore to reduce load on zookeeper.
 */
public class KeyCoordinatorVersionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorVersionManager.class);

  private static final String VERSION_PRODUCED_ZN_NAME = "VERSION_PRODUCED";
  private static final String VERSION_PRODUCED_KEY = "VERSION_PRODUCED";

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;

  public KeyCoordinatorVersionManager(HelixManager helixManager) {
    _propertyStore = helixManager.getHelixPropertyStore();
  }

  public synchronized long getVersionProducedFromPropertyStore() {
    ZNRecord record = _propertyStore.get(VERSION_PRODUCED_ZN_NAME, null, AccessOption.PERSISTENT);
    return Long.parseLong(record.getSimpleField(VERSION_PRODUCED_KEY));
  }

  public synchronized void setVersionProducedToPropertyStore(long versionProduced) {
    ZNRecord record = getZNRecordForVersionProduced(versionProduced);
    _propertyStore.set(VERSION_PRODUCED_ZN_NAME, record, AccessOption.PERSISTENT);
  }

  private ZNRecord getZNRecordForVersionProduced(long versionProduced) {
    ZNRecord record = new ZNRecord(VERSION_PRODUCED_ZN_NAME);
    record.setLongField(VERSION_PRODUCED_KEY, versionProduced);
    return record;
  }
}

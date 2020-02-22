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
package org.apache.pinot.grigio.keyCoordinator.helix;

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manager for version numbers. This handles getting/setting version produced/consumed from/to zookeeper
 * property store. Utilizes cache built in ZkHelixPropertyStore to reduce load on zookeeper.
 */
public class KeyCoordinatorVersionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorVersionManager.class);

  private static final String VERSION_PRODUCED_ZN_PATH = "/VERSION_PRODUCED";
  private static final String VERSION_PRODUCED_KEY = "VERSION_PRODUCED";
  private static final String VERSION_CONSUMED_ZN_PATH = "/VERSION_CONSUMED";

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String _instanceName;

  public KeyCoordinatorVersionManager(HelixManager helixManager) {
    _propertyStore = helixManager.getHelixPropertyStore();
    _instanceName = helixManager.getInstanceName();
  }

  /**
   * Get version produced for the key coordinator cluster.
   *
   * There is only one version produced for the whole key coordinator cluster.
   */
  public long getVersionProducedFromPropertyStore() {
    ZNRecord record = _propertyStore.get(VERSION_PRODUCED_ZN_PATH, null, AccessOption.PERSISTENT);
    if (record == null) {
      // new cluster
      return 0L;
    }
    return Long.parseLong(record.getSimpleField(VERSION_PRODUCED_KEY));
  }

  /**
   * Set version produced for the key coordinator cluster.
   *
   * There is only one version produced for the whole key coordinator cluster.
   *
   * @return true if the version produced is saved to the property store successfully, false otherwise.
   */
  public boolean setVersionProducedToPropertyStore(long versionProduced) {
    ZNRecord record = new ZNRecord(VERSION_PRODUCED_ZN_PATH);
    record.setLongField(VERSION_PRODUCED_KEY, versionProduced);
    return _propertyStore.set(VERSION_PRODUCED_ZN_PATH, record, AccessOption.PERSISTENT);
  }

  /**
   * Get version consumed for the current key coordinator instance.
   *
   * There is a map of version consumed for each key coordinator instance, with the partition as key and version as value.
   */
  public Map<Integer, Long> getVersionConsumedFromPropertyStore() {
    Map<Integer, Long> versionConsumed = new HashMap<>();
    ZNRecord record = _propertyStore.get(VERSION_CONSUMED_ZN_PATH, null, AccessOption.PERSISTENT);
    if (record == null) {
      // new cluster
      return versionConsumed;
    }
    Map<String, String> versionConsumedStr = record.getMapField(_instanceName);
    if (versionConsumedStr == null) {
      // new instance
      return versionConsumed;
    }
    for (Map.Entry<String, String> entry : versionConsumedStr.entrySet()) {
      versionConsumed.put(Integer.parseInt(entry.getKey()), Long.parseLong(entry.getValue()));
    }
    return versionConsumed;
  }

  /**
   * Set the version consumed for the current key coordinator instance.
   *
   * There is a map of version consumed for each key coordinator instance, with the partition as key and version as value.
   *
   * @return true if the version consumed is saved to the property store successfully, false otherwise.
   */
  public boolean setVersionConsumedToPropertyStore(Map<Integer, Long> versionConsumed) {
    ZNRecord record = _propertyStore.get(VERSION_CONSUMED_ZN_PATH, null, AccessOption.PERSISTENT);
    if (record == null) {
      record = new ZNRecord(VERSION_CONSUMED_ZN_PATH);
    }
    Map<String, String> versionConsumedStr = new HashMap<>();
    for (Map.Entry<Integer, Long> entry : versionConsumed.entrySet()) {
      versionConsumedStr.put(entry.getKey().toString(), entry.getValue().toString());
    }
    record.setMapField(_instanceName, versionConsumedStr);
    return _propertyStore.set(VERSION_CONSUMED_ZN_PATH, record, AccessOption.PERSISTENT);
  }
}

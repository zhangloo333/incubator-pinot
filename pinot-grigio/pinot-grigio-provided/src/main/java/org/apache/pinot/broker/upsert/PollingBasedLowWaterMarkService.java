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
package org.apache.pinot.broker.upsert;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkCacheBaseDataAccessor;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.restlet.resources.TableLowWaterMarksInfo;
import org.apache.pinot.core.segment.updater.LowWaterMarkService;
import org.glassfish.jersey.client.ClientProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// A low water mark service which polls various Pinot servers periodically to get the low water marks for partitions of
// servers.
public class PollingBasedLowWaterMarkService implements LowWaterMarkService {
  private static final Logger LOGGER = LoggerFactory.getLogger(PollingBasedLowWaterMarkService.class);

  private static final String LWMS_PATH = "lwms";
  private static final String HTTP = "http";

  private static final int SERVER_CONNENCT_TIMEOUT_MS = 10000;
  private static final int SERVER_READ_TIMEOUT = 10000;
  private static final String SERVER_PREFIX = "Server_";

  // A map from table_name to its partition->lwm mapping.
  private Map<String, Map<Integer, Long>> _tableLowWaterMarks;
  private ZkCacheBaseDataAccessor<ZNRecord> _cacheInstanceConfigsDataAccessor;
  private Client _httpClient;
  // We can tune this polling interval to make sure we get the fresh snapshot of server low water marks.
  private int _serverPollingInterval;
  private int _serverPort;
  private boolean _shuttingDown;
  private BrokerMetrics _brokerMetrics;

  @Override
  public void init(HelixDataAccessor helixDataAccessor, String helixClusterName, int serverPollingInterval, int serverPort) {
    // Construct the zk path to get the server instances.
    String instanceConfigs = PropertyPathBuilder.instanceConfig(helixClusterName);
    // Build a zk data reader.
    _cacheInstanceConfigsDataAccessor =
        new ZkCacheBaseDataAccessor<>((ZkBaseDataAccessor<ZNRecord>) helixDataAccessor.getBaseDataAccessor(),
            instanceConfigs, null, Collections.singletonList(instanceConfigs));
    _tableLowWaterMarks = new ConcurrentHashMap<>();
    _httpClient = ClientBuilder.newClient();
    _httpClient.property(ClientProperties.CONNECT_TIMEOUT, SERVER_CONNENCT_TIMEOUT_MS);
    _httpClient.property(ClientProperties.READ_TIMEOUT, SERVER_READ_TIMEOUT);
    _serverPollingInterval = serverPollingInterval;
    _serverPort = serverPort;
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          shutDown();
        } catch (final Exception e) {
          LOGGER.error("Caught exception while running shutdown hook", e);
        }
      }
    });
  }

  @Override
  public void start(BrokerMetrics brokerMetrics) {
    _brokerMetrics = brokerMetrics;
    Thread serverPollingThread = new Thread(new PinotServerPollingExecutor());
    serverPollingThread.start();
  }

  @Override
  public Map<Integer, Long> getLowWaterMarks(String tableName) {
    return _tableLowWaterMarks.get(tableName);
  }

  @Override
  public void shutDown() {
    _shuttingDown = true;
  }

  // Poll all the servers periodically to find out the Low Water Mark info.
  private class PinotServerPollingExecutor implements Runnable {
    @Override
    public void run() {
      while (!_shuttingDown) {
        try {
          Map<String, Map<Integer, Long>> latestLowWaterMarks = new ConcurrentHashMap<>();
          // 1. Find out all the alive servers.
          List<String> serverInstances = _cacheInstanceConfigsDataAccessor.getChildNames("/", AccessOption.PERSISTENT);
          List<ZNRecord> instances = _cacheInstanceConfigsDataAccessor.getChildren("/", null, AccessOption.PERSISTENT);
          for (ZNRecord r : instances) {
            LOGGER.info("Instance info for lwms: {}", r.toString());
          }
          // 2. Ask each server for its low water mark info.
          for (String serverIntanceId : serverInstances) {
            // Check the instance is in fact a server.
            if (!serverIntanceId.startsWith(SERVER_PREFIX) && !serverIntanceId.startsWith("server_"))
              continue;
            InstanceConfig serverConfig = InstanceConfig.toInstanceConfig(serverIntanceId.substring(
                SERVER_PREFIX.length()));
            try {
              // (TODO) Fixing this. Hardcode using the default server admin port for now.
              WebTarget webTarget = _httpClient.target(getURI(serverConfig.getHostName(), _serverPort));
              TableLowWaterMarksInfo lwms = webTarget.path(PollingBasedLowWaterMarkService.LWMS_PATH).request().
                  get(TableLowWaterMarksInfo.class);
              LOGGER.info("Found low water mark info for server {}: {}", serverIntanceId, lwms.getTableLowWaterMarks());
              // 3. Update the low water marks.
              LwmMerger.updateLowWaterMarks(latestLowWaterMarks, lwms.getTableLowWaterMarks());
            } catch (Exception e) {
              // TODO(tingchen) Handle server failures. We could keep the last known lwms of a server.
              LOGGER.warn("Error during getting low water marks from server {}", serverIntanceId, e);
            }
          }
          // 4. Replace the broker's low water marks table with the latest low water mark info.
          if (validate(latestLowWaterMarks)) {
            _tableLowWaterMarks = latestLowWaterMarks;
          }
          // 5. Sleep for some interval.
          Thread.sleep(_serverPollingInterval);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          // It is OK for us to break out the loop early because the Low Water Mark refresh is best effort.
          break;
        }
      }
    }

    // Validate the low water mark info polled from all the servers are right. For now, return true.
    // (TODO tingchen) figure out the right checks.
    private boolean validate(Map<String, Map<Integer, Long>> latestLowWaterMarks) {
      if (latestLowWaterMarks == null) {
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.LOW_WATER_MARK_QUERY_FAILURES, 1);
        return false;
      }
      for(String tableName : latestLowWaterMarks.keySet()) {
        Map<Integer, Long> partitionLWMs = latestLowWaterMarks.get(tableName);
        _brokerMetrics.addValueToTableGauge(tableName, BrokerGauge.TABLE_MIN_LOW_WATER_MARK,
            Collections.min(partitionLWMs.values()));
      }
      return true;
    }

    private URI getURI(String host, int port) throws URISyntaxException {
      LOGGER.info("requesting host {} and port {}", host, port);
      return new URI(PollingBasedLowWaterMarkService.HTTP, null, host, port, null
          , null, null);
    }
  }

  static class LwmMerger {
    // Update an existing map currentLwmsMap of tableName->low_water_marks with a new map of the same type.
    // If an entry in the new map does not exist in currentLwmsMap, insert it to currentLwmsMap.
    // otherwise merge the entry with the existing entry in currentLwmsMap using mergeTablePartitionLwms().
    static void updateLowWaterMarks(Map<String, Map<Integer, Long>> currentLwmsMap,
                                    final Map<String, Map<Integer, Long>> serverLwmsMap) {
      for (Map.Entry<String, Map<Integer, Long>> serverMap : serverLwmsMap.entrySet()) {
        String tableName = serverMap.getKey();
        Map<Integer, Long> tableLwms = serverMap.getValue();
        if (currentLwmsMap.containsKey(tableName)) {
          currentLwmsMap.put(tableName,
              LwmMerger.mergeTablePartitionLwms(Collections.unmodifiableMap(currentLwmsMap.get(tableName)),
                  tableLwms));
        } else {
          currentLwmsMap.put(tableName, tableLwms);
        }
      }
    }

    // Merge all the entries in the two input maps of partition_id->lwm.
    // If an entry exists only in a map, put it in the combined map.
    // If an entry exists in both maps, use the entry with the smaller low water marks.
    static Map<Integer, Long> mergeTablePartitionLwms(final Map<Integer, Long> m1, final Map<Integer, Long> m2) {
      if (m1 == null || m1.size() == 0) {
        return m2;
      }
      if (m2 == null || m2.size() == 0) {
        return m1;
      }
      Map<Integer, Long> result = new HashMap<>(m1);
      for (Map.Entry<Integer, Long> entry : m2.entrySet()) {
        Integer partitionNo = entry.getKey();
        Long lwm = entry.getValue();
        if (result.containsKey(partitionNo)) {
          result.put(partitionNo, Math.min(lwm, result.get(partitionNo)));
        } else {
          result.put(partitionNo, lwm);
        }
      }
      return result;
    }
  }
}

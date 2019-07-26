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

import com.google.common.collect.Sets;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.model.ExternalView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.pinot.common.utils.CommonConstants.Helix.BROKER_RESOURCE_INSTANCE;
import static org.apache.pinot.common.utils.CommonConstants.Helix.TableType.REALTIME;

/**
 * Tracks all the changes happening in the Pinot cluster for all external views
 * Keeps a track of existing tables within the KeyCoordinator and uses this
 * to notify which tables are being newly added and which tables are being
 * deleted.
 */
public class ActiveTableTracker implements ExternalViewChangeListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(ActiveTableTracker.class);

    private final Set<String> _activeTables = new HashSet<>();
    private final TableListener _listener;

    /**
     * Initialize with any existing tables. Any table added / deleted is then
     * notified after comparing to these tables.
     *
     * @param existingTables specifies tables that already exist in the Key Coordinator
     * @param listener register a listener for the table add/delete callbacks
     */
    public ActiveTableTracker(Set<String> existingTables, TableListener listener) {
        if (existingTables != null) {
            _activeTables.addAll(existingTables);
        }
        _listener = listener;
    }

    public Set<String> getCurrentActiveTables() {
        return Collections.unmodifiableSet(_activeTables);
    }

    @Override
    public void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext notificationContext) {
        Set<String> externalViewTables = new HashSet<>();

        // First, create a set of all table names seen in the external view
        for (ExternalView view : externalViewList) {

            // For table creation / deletion its sufficient to listen to external
            // view for 'brokerResource'. Other views contain segment information
            // which is not needed at this point.
            if (BROKER_RESOURCE_INSTANCE.equalsIgnoreCase(view.getResourceName())) {
                for (String tableName : view.getPartitionSet()) {
                    // We're only interested in tracking realtime tables
                    if (!tableName.endsWith(REALTIME.name())) {
                        continue;
                    }
                    LOGGER.debug("Found table in external view: {}", tableName);
                    externalViewTables.add(tableName);
                }
                break;
            }
        }

        Set<String> newTablesAdded = new HashSet<>();
        Set<String> tablesDeleted = new HashSet<>();
        // Now compute the sets of tableNames being newly added and deleted
        newTablesAdded.addAll(Sets.difference(externalViewTables, _activeTables));
        tablesDeleted.addAll(Sets.difference(_activeTables, externalViewTables));

        // Finally, track these changes in the internal set and send
        // notifications as needed
        newTablesAdded.forEach(tableName -> {
            LOGGER.info("Adding new table: {}", tableName);
            _activeTables.add(tableName);
            _listener.onNewTableCreated(tableName);
        });

        tablesDeleted.forEach(tableName -> {
            LOGGER.info("Deleting table: {}", tableName);
            _activeTables.remove(tableName);
            _listener.onTableDeleted(tableName);
        });
    }
}

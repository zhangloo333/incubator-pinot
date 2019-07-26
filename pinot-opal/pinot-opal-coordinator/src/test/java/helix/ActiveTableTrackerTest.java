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
package helix;

import org.apache.pinot.opal.keyCoordinator.helix.ActiveTableTracker;
import org.apache.pinot.opal.keyCoordinator.helix.TableListener;
import org.apache.helix.model.ExternalView;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.apache.pinot.common.utils.CommonConstants.Helix.BROKER_RESOURCE_INSTANCE;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.fail;

public class ActiveTableTrackerTest {

    private final String _tableName = "dummy_table_REALTIME";
    private final String _brokerInstanceId = "broker_instance_1";
    private ExternalView _brokerResource;
    private ExternalView _brokerResourceEmpty;

    @BeforeClass
    public void setUp() {
        _brokerResource = new ExternalView(BROKER_RESOURCE_INSTANCE);
        _brokerResource.setState(_tableName, _brokerInstanceId, "ONLINE");
        _brokerResourceEmpty = new ExternalView(BROKER_RESOURCE_INSTANCE);
    }

    @Test
    public void testNewTableCreation() throws Exception {
        final boolean[] success = {false};
        ActiveTableTracker tracker = new ActiveTableTracker(new HashSet<>(), new TableListener() {
            @Override
            public void onNewTableCreated(String tableName) {
                assertEquals(tableName, _tableName);
                success[0] = true;
            }

            @Override
            public void onTableDeleted(String tableName) {
            }
        });
        List<ExternalView> externalViewList = Arrays.asList(_brokerResource);
        tracker.onExternalViewChange(externalViewList, null);
        if (!checkResultWithSleep(success)) {
            fail("Did not get a callback .");
        }
    }

    @Test
    public void testTableDeletion() throws Exception {
        final boolean[] success = {false};
        ActiveTableTracker tracker = new ActiveTableTracker(new HashSet<>(), new TableListener() {
            @Override
            public void onNewTableCreated(String tableName) {
                assertEquals(tableName, _tableName);
            }

            @Override
            public void onTableDeleted(String tableName) {
                assertEquals(tableName, _tableName);
                success[0] = true;
            }
        });

        // Lets first add a table
        List<ExternalView> externalViewList = Arrays.asList(_brokerResource);
        tracker.onExternalViewChange(externalViewList, null);

        // Now send a deletion event
        List<ExternalView> externalViewListDeletion = Arrays.asList(_brokerResourceEmpty);
        tracker.onExternalViewChange(externalViewListDeletion, null);

        if (!checkResultWithSleep(success)) {
            fail("Did not get a callback .");
        }
    }

    private boolean checkResultWithSleep(final boolean[] success) {
        try {
            for(int i=0;i<10;i++) {
                Thread.sleep(1000);
                if (success[0]) {
                    return true;
                }
            }
        } catch (Exception e) {}
        return false;
    }
}
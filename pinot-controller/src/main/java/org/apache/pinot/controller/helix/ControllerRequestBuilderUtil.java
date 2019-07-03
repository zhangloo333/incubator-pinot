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
package org.apache.pinot.controller.helix;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pinot.common.config.Tenant;
import org.apache.pinot.common.config.Tenant.TenantBuilder;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.common.utils.TenantRole;

import static org.apache.pinot.common.utils.CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE;
import static org.apache.pinot.common.utils.CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE;


public class ControllerRequestBuilderUtil {

  public static String buildBrokerTenantCreateRequestJSON(String tenantName, int numberOfInstances)
      throws JsonProcessingException {
    Tenant tenant = new TenantBuilder(tenantName).setRole(TenantRole.BROKER).setTotalInstances(numberOfInstances)
        .setOfflineInstances(0).setRealtimeInstances(0).build();
    return JsonUtils.objectToString(tenant);
  }

  public static String buildServerTenantCreateRequestJSON(String tenantName, int numberOfInstances,
      int offlineInstances, int realtimeInstances)
      throws JsonProcessingException {
    Tenant tenant = new TenantBuilder(tenantName).setRole(TenantRole.SERVER).setTotalInstances(numberOfInstances)
        .setOfflineInstances(offlineInstances).setRealtimeInstances(realtimeInstances).build();
    return JsonUtils.objectToString(tenant);
  }
}

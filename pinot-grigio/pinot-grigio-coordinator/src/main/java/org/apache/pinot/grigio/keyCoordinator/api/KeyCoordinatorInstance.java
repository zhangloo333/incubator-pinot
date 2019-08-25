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
package org.apache.pinot.grigio.keyCoordinator.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.CommonConstants;


public class KeyCoordinatorInstance {
  private final String _host;
  private final String _port;

  @JsonCreator
  public KeyCoordinatorInstance(
      @JsonProperty(value = "host", required = true) String host,
      @JsonProperty(value = "port", required = true) String port
  ) {
    _host = host;
    _port = port;
  }

  public String getHost() {
    return _host;
  }

  public String getPort() {
    return _port;
  }

  public String toInstanceId() {
    return CommonConstants.Helix.PREFIX_OF_KEY_COORDINATOR_INSTANCE + _host + "_" + _port;
  }

  @Override
  public String toString() {
    final StringBuilder bld = new StringBuilder();
    bld.append("host : " + _host + "\n");
    bld.append("port : " + _port + "\n");
    return bld.toString();
  }

  public InstanceConfig toInstanceConfig() {
    final InstanceConfig iConfig = new InstanceConfig(toInstanceId());
    iConfig.setHostName(_host);
    iConfig.setPort(_port);
    iConfig.setInstanceEnabled(true);
    return iConfig;
  }
}

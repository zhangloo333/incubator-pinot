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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.helix.HelixDataAccessor;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.core.segment.updater.LowWaterMarkService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.common.utils.CommonConstants.Broker.*;

/**
 * provider to initialize LowWaterMarkServer for pinot broker
 */
public class LowWaterMarkServiceProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(LowWaterMarkServiceProvider.class);

  private LowWaterMarkService _instance;

  /**
   * create a new provider instance
   * @param brokerConfig config for this provider to create the actual class reference,
   *                     refer to {@value CommonConstants.Broker#CONFIG_OF_BROKER_LWMS_CLASS_NAME}
   * @param dataAccessor helix data access to help low watermark service to find proper server cluster
   * @param clusterName cluster name for the current pinot cluster
   */
  public LowWaterMarkServiceProvider(Configuration brokerConfig, HelixDataAccessor dataAccessor, String clusterName) {
    String className = brokerConfig.getString(CommonConstants.Broker.CONFIG_OF_BROKER_LWMS_CLASS_NAME,
        DefaultLowWaterMarkService.class.getName());
    LOGGER.info("creating watermark manager with class {}", className);
    try {
      Class<LowWaterMarkService> comonentContainerClass = (Class<LowWaterMarkService>) Class.forName(className);
      _instance = comonentContainerClass.newInstance();
      _instance.init(dataAccessor, clusterName,
          brokerConfig.getInt(CONFIG_OF_BROKER_POLLING_SERVER_LWMS_INTERVAL_MS,
              DEFAULT_OF_BROKER_POLLING_SERVER_LWMS_INTERVAL_MS),
          brokerConfig.getInt(CONFIG_OF_BROKER_POLLING_SERVER_LWMS_SERVER_PORT,
              CommonConstants.Server.DEFAULT_ADMIN_API_PORT));
    } catch (Exception e) {
      LOGGER.error("failed to load watermark manager class", className, e);
      _instance = null;
      ExceptionUtils.rethrow(e);
    }
  }

  /**
   * fetch the current instance of low watermark service this provider created
   * @return
   */
  public LowWaterMarkService getInstance() {
    return _instance;
  }
}

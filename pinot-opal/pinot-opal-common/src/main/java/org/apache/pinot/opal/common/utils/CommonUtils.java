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
package org.apache.pinot.opal.common.utils;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class CommonUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(CommonUtils.class);

  public static void printConfiguration(Configuration configuration, String name) {
    LOGGER.info("printing {} configuration", name);
    configuration.getKeys().forEachRemaining(key -> {
      LOGGER.info("{}: {}", key, configuration.getProperty((String)key));
    });
  }

  public static Properties getPropertiesFromConf(Configuration conf) {
    return ConfigurationConverter.getProperties(conf);
  }

  public static String getTableNameFromKafkaTopic(String topic, String topicPrefix) {
    Preconditions.checkState(topic.length() > topicPrefix.length(), "kafka topic is not valid");
    return topic.substring(topicPrefix.length());
  }

}

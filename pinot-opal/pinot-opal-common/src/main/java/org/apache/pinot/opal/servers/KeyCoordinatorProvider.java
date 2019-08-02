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
package org.apache.pinot.opal.servers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.Utils;
import org.apache.pinot.opal.common.config.CommonConfig;
import org.apache.pinot.opal.common.rpcQueue.QueueProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.pinot.opal.common.config.CommonConfig.RPC_QUEUE_CONFIG.CLASS_NAME;
import static org.apache.pinot.opal.common.config.CommonConfig.RPC_QUEUE_CONFIG.PRODUCER_CONFIG_KEY;


public class KeyCoordinatorProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorProvider.class);

  @VisibleForTesting
  protected static KeyCoordinatorProvider _instance = null;

  private String _className;
  private Configuration _producerConf;
  private Map<String, QueueProducer> _cachedProducerMap = new HashMap<>();
  private volatile boolean _isClosed = false;

  public KeyCoordinatorProvider(Configuration conf, String hostname) {
    Preconditions.checkState(StringUtils.isNotEmpty(hostname), "host name should not be empty");
    _producerConf = conf.subset(PRODUCER_CONFIG_KEY);
    _className = _producerConf.getString(CLASS_NAME);
    Preconditions.checkState(StringUtils.isNotEmpty(_className),
        "key coordinator producer class should not be empty");
    _producerConf.addProperty(CommonConfig.RPC_QUEUE_CONFIG.HOSTNAME_KEY, hostname);

    synchronized (KeyCoordinatorProvider.class) {
      if (_instance == null) {
        _instance = this;
      } else {
        throw new RuntimeException("cannot re-initialize key coordinator provide when there is already one instance");
      }
    }
  }

  public static KeyCoordinatorProvider getInstance() {
    if (_instance != null) {
      return _instance;
    } else {
      throw new RuntimeException("cannot get instance of key coordinator provider without initializing one before");
    }
  }

  public synchronized QueueProducer getCachedProducer(String tableName) {
    Preconditions.checkState(!_isClosed, "provider should not be closed");
    return _cachedProducerMap.computeIfAbsent(tableName, t -> createNewProducer());
  }

  private synchronized QueueProducer createNewProducer() {
    QueueProducer producer = null;
    try {
      producer = (QueueProducer) Class.forName(_className).newInstance();
      producer.init(_producerConf);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
      LOGGER.info("failed to load/create class for key coordinator producer for class {}", _className);
      Utils.rethrowException(ex);
    }
    return producer;
  }

  public synchronized void close() {
    _isClosed = true;
    _cachedProducerMap.values().forEach(QueueProducer::close);
  }
}

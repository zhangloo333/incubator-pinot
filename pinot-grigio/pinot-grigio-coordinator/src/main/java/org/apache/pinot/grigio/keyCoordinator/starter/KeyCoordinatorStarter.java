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
package org.apache.pinot.grigio.keyCoordinator.starter;

import com.google.common.base.Preconditions;
import com.yammer.metrics.core.MetricsRegistry;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.metrics.MetricsHelper;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.grigio.common.config.CommonConfig;
import org.apache.pinot.grigio.common.rpcQueue.KeyCoordinatorQueueConsumer;
import org.apache.pinot.grigio.common.rpcQueue.KeyCoordinatorQueueProducer;
import org.apache.pinot.grigio.common.rpcQueue.LogCoordinatorQueueProducer;
import org.apache.pinot.grigio.common.rpcQueue.VersionMsgQueueProducer;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogStorageProvider;
import org.apache.pinot.grigio.common.updateStrategy.MessageResolveStrategy;
import org.apache.pinot.grigio.common.updateStrategy.MessageTimeResolveStrategy;
import org.apache.pinot.grigio.keyCoordinator.GrigioKeyCoordinatorMetrics;
import org.apache.pinot.grigio.keyCoordinator.api.KeyCoordinatorApiApplication;
import org.apache.pinot.grigio.keyCoordinator.helix.KeyCoordinatorClusterHelixManager;
import org.apache.pinot.grigio.keyCoordinator.helix.State;
import org.apache.pinot.grigio.keyCoordinator.internal.DistributedKeyCoordinatorCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Iterator;

public class KeyCoordinatorStarter {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorStarter.class);

  private KeyCoordinatorConf _keyCoordinatorConf;
  private GrigioKeyCoordinatorMetrics _metrics;
  private KeyCoordinatorQueueConsumer _consumer;
  private LogCoordinatorQueueProducer _producer;
  private VersionMsgQueueProducer _versionMessageProducer;
  private MessageResolveStrategy _messageResolveStrategy;
  private DistributedKeyCoordinatorCore _keyCoordinatorCore;
  private KeyCoordinatorApiApplication _application;
  private String _hostName;
  private int _port;
  private String _instanceId;
  private KeyCoordinatorClusterHelixManager _keyCoordinatorClusterHelixManager;

  public KeyCoordinatorStarter(KeyCoordinatorConf conf) throws Exception {
    _keyCoordinatorConf = conf;
    initMetrics(_keyCoordinatorConf.getMetricsConf(), _keyCoordinatorConf.getMetricsPrefix());
    _hostName = conf.getString(KeyCoordinatorConf.HOST_NAME);
    Preconditions.checkState(StringUtils.isNotEmpty(_hostName), "expect host name in configuration");
    _port = conf.getPort();
    _instanceId = CommonConstants.Helix.PREFIX_OF_KEY_COORDINATOR_INSTANCE + _hostName + "_" + _port;
    _consumer = getConsumer(_keyCoordinatorConf.getConsumerConf());
    _producer = getProducer(_keyCoordinatorConf.getProducerConf());
    _versionMessageProducer = getVersionMessageProducer(_keyCoordinatorConf.getVersionMessageProducerConf());
    _keyCoordinatorClusterHelixManager = new KeyCoordinatorClusterHelixManager(
        _keyCoordinatorConf.getZkStr(),
        _keyCoordinatorConf.getKeyCoordinatorClusterName(),
        _instanceId,
        _consumer,
        conf.getKeyCoordinatorMessageTopic(),
        conf.getKeyCoordinatorMessagePartitionCount()
    );
    UpdateLogStorageProvider.init(_keyCoordinatorConf.getStorageProviderConf());
    _messageResolveStrategy = new MessageTimeResolveStrategy();
    _keyCoordinatorCore = new DistributedKeyCoordinatorCore();
    _application = new KeyCoordinatorApiApplication(this);
  }

  private void initMetrics(Configuration conf, String prefix) {
    MetricsHelper.initializeMetrics(conf);
    MetricsRegistry registry = new MetricsRegistry();
    MetricsHelper.registerMetricsRegistry(registry);
    _metrics = new GrigioKeyCoordinatorMetrics(prefix, registry);
    _metrics.initializeGlobalMeters();
  }

  private KeyCoordinatorQueueConsumer getConsumer(Configuration consumerConfig) {
    consumerConfig.setProperty(CommonConfig.RPC_QUEUE_CONFIG.HOSTNAME_KEY, _hostName);
    KeyCoordinatorQueueConsumer consumer = new KeyCoordinatorQueueConsumer();
    consumer.init(consumerConfig, _metrics);
    return consumer;
  }

  private LogCoordinatorQueueProducer getProducer(Configuration producerConfig) {
    producerConfig.setProperty(CommonConfig.RPC_QUEUE_CONFIG.HOSTNAME_KEY, _hostName);
    LogCoordinatorQueueProducer producer = new LogCoordinatorQueueProducer();
    producer.init(producerConfig, _metrics);
    return producer;
  }

  private VersionMsgQueueProducer getVersionMessageProducer(Configuration configuration) {
    configuration.setProperty(CommonConfig.RPC_QUEUE_CONFIG.HOSTNAME_KEY, _hostName);
    VersionMsgQueueProducer versionMessageProducer = new VersionMsgQueueProducer();
    versionMessageProducer.init(configuration, _metrics);
    return versionMessageProducer;
  }

  public KeyCoordinatorClusterHelixManager getKeyCoordinatorClusterHelixManager() {
    return _keyCoordinatorClusterHelixManager;
  }

  public KeyCoordinatorQueueConsumer getConsumer() {
    return _consumer;
  }

  public void start() {
    LOGGER.info("starting key coordinator instance");
    _keyCoordinatorCore
        .init(_keyCoordinatorConf, _producer, _consumer, _versionMessageProducer, _messageResolveStrategy,
            _keyCoordinatorClusterHelixManager, _metrics);
    LOGGER.info("finished init key coordinator instance, starting loop");
    _keyCoordinatorCore.start();
    LOGGER.info("starting web service");
    _application.start(_port);
  }

  public void shutdown() {
    LOGGER.info("shutting down key coordinator instance");
    _keyCoordinatorCore.stop();
    LOGGER.info("finished shutdown key coordinator instance");
    _producer.close();
    LOGGER.info("finished shutdown producer");
    _consumer.close();
    LOGGER.info("finished shutdown consumer");
    _versionMessageProducer.close();
    LOGGER.info("finished shutdown version message producer");
  }

  public boolean isRunning() {
    return _keyCoordinatorCore != null && _keyCoordinatorCore.getState() == State.RUNNING;
  }

  public static KeyCoordinatorStarter startDefault(KeyCoordinatorConf conf) throws Exception {
    KeyCoordinatorStarter starter = new KeyCoordinatorStarter(conf);
    starter.start();
    return starter;
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.out.println("need path to file in props");
    }
    File confFile = new File(args[0]);
    if (!confFile.exists()) {
      System.out.println("conf file does not exist");
    }
    KeyCoordinatorConf properties = new KeyCoordinatorConf(confFile);
    LOGGER.info(properties.toString());
    Iterator<String> iterators = properties.getKeys();
    while (iterators.hasNext()) {
      String key = iterators.next();
      LOGGER.info("grigio kc Prop: key= " + key + ", value= " + properties.getString(key));
    }
    KeyCoordinatorStarter starter = startDefault(properties);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          LOGGER.info("received shutdown event from shutdown hook");
          starter.shutdown();
        } catch (Exception e) {
          e.printStackTrace();
          LOGGER.error("error shutting down key coordinator: ", e);
        }
      }
    });

  }
}

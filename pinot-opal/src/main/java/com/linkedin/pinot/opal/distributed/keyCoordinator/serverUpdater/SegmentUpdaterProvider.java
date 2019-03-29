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
package com.linkedin.pinot.opal.distributed.keyCoordinator.serverUpdater;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Component to provide the segment updater for related components
 * Right now it provide the consumer to ingest data from key coordinator output queue
 */
public class SegmentUpdaterProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentUpdaterProvider.class);
  private static SegmentUpdaterProvider _instance = null;

  private Configuration _conf;
  private SegmentUpdateQueueConsumer _consumer;

  public SegmentUpdaterProvider(Configuration conf, String hostName) {
    Preconditions.checkState(StringUtils.isNotEmpty(hostName), "host name should not be empty");
    _conf = conf;
    _consumer = new SegmentUpdateQueueConsumer(conf.subset(SegmentUpdaterQueueConfig.CONSUMER_CONFIG), hostName);
    synchronized (SegmentUpdaterProvider.class) {
      if (_instance == null) {
        _instance = this;
      } else {
        throw new RuntimeException("cannot re-initialize segment updater provide when there is already one instance");
      }
    }
  }

  public static SegmentUpdaterProvider getInstance() {
    if (_instance != null) {
      return _instance;
    } else {
      throw new RuntimeException("cannot get instance of segment updater provider without initializing one before");
    }

  }

  public SegmentUpdateQueueConsumer getConsumer() {
    return _consumer;
  }

  public void close() {
    //TODO close producer and what not
    _consumer.close();
  }
}

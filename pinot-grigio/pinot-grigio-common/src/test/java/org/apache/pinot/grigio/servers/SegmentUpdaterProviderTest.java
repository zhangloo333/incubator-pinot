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
package org.apache.pinot.grigio.servers;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.grigio.common.metrics.MockGrigioMetrics;
import org.apache.pinot.grigio.common.metrics.GrigioMetrics;
import org.apache.pinot.grigio.common.rpcQueue.QueueConsumer;
import org.apache.pinot.grigio.common.rpcQueue.QueueConsumerRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.pinot.grigio.common.config.CommonConfig.RPC_QUEUE_CONFIG.CLASS_NAME;
import static org.apache.pinot.grigio.common.config.CommonConfig.RPC_QUEUE_CONFIG.CONSUMER_CONFIG_KEY;
import static org.apache.pinot.grigio.common.config.CommonConfig.RPC_QUEUE_CONFIG.HOSTNAME_KEY;

public class SegmentUpdaterProviderTest {

  private Configuration conf = new PropertiesConfiguration();

  @BeforeTest
  public void setup() {
    conf.addProperty(CONSUMER_CONFIG_KEY + "." + CLASS_NAME, MockConsumer.class.getName());
  }

  @Test
  public void testGetConsumer() {
    SegmentUpdaterProvider provider = new SegmentUpdaterProvider(conf, "host_name_sample", new MockGrigioMetrics());
    Configuration conf = ((MockConsumer) provider.getConsumer())._conf;

    Assert.assertEquals(conf.getString(HOSTNAME_KEY), "host_name_sample");
    Assert.assertEquals(conf.getString(CLASS_NAME), MockConsumer.class.getName());
    Assert.assertEquals(SegmentUpdaterProvider.getInstance(), provider);

    // verify close logic
    Assert.assertEquals(((MockConsumer)provider.getConsumer())._isClosed, false);
    provider.close();
    Assert.assertEquals(((MockConsumer)provider.getConsumer())._isClosed, true);
  }

  static class MockConsumer implements QueueConsumer {
    protected Configuration _conf;
    protected boolean _isClosed = false;

    @Override
    public void init(Configuration conf, GrigioMetrics metrics) {
      _conf = conf;
    }

    @Override
    public void subscribeForTable(String table) {
    }

    @Override
    public void unsubscribeForTable(String table) {
    }

    @Override
    public List<QueueConsumerRecord> getRequests(long timeout, TimeUnit timeUnit) {
      return null;
    }

    @Override
    public void ackOffset() {
    }

    @Override
    public void close() {
      _isClosed = true;

    }
  }
}
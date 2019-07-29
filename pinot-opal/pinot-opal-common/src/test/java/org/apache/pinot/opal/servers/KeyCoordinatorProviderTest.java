package org.apache.pinot.opal.servers;
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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.opal.common.rpcQueue.ProduceTask;
import org.apache.pinot.opal.common.rpcQueue.QueueProducer;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.List;

import static org.apache.pinot.opal.common.config.CommonConfig.RPC_QUEUE_CONFIG.CLASS_NAME;
import static org.apache.pinot.opal.common.config.CommonConfig.RPC_QUEUE_CONFIG.HOSTNAME_KEY;
import static org.apache.pinot.opal.common.config.CommonConfig.RPC_QUEUE_CONFIG.PRODUCER_CONFIG_KEY;

public class KeyCoordinatorProviderTest {

  private Configuration conf = new PropertiesConfiguration();

  @BeforeTest
  public void setup() {

    conf.addProperty(PRODUCER_CONFIG_KEY + "." + CLASS_NAME, MockProducer.class.getName());
  }

  @Test
  public void testCreteProucer() {
    KeyCoordinatorProvider provider = new KeyCoordinatorProvider(conf, "host_name_sample");

    Configuration producerConfig = ((MockProducer)provider.getProducer())._conf;

    Assert.assertEquals(producerConfig.getString(HOSTNAME_KEY), "host_name_sample");
    Assert.assertEquals(producerConfig.getString(CLASS_NAME), MockProducer.class.getName());
    Assert.assertEquals(KeyCoordinatorProvider.getInstance(), provider);

    // verify close logic
    Assert.assertEquals(((MockProducer)provider.getProducer())._isClosed, false);
    provider.close();
    Assert.assertEquals(((MockProducer)provider.getProducer())._isClosed, true);
  }


  static class MockProducer implements QueueProducer {
    protected Configuration _conf;
    protected boolean _isClosed = false;

    @Override
    public void init(Configuration conf) {
      _conf = conf;


    }

    @Override
    public void produce(ProduceTask task) {

    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {
      _isClosed = true;

    }

    @Override
    public void batchProduce(List list) {

    }
  }
}
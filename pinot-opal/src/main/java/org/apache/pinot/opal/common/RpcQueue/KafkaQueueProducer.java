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
package org.apache.pinot.opal.common.RpcQueue;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;

public abstract class KafkaQueueProducer<K, V> implements QueueProducer<K, V>{

  protected abstract KafkaProducer<K, V> getKafkaNativeProducer();

  protected abstract String getDefaultTopic();

  @Override
  public void produce(ProduceTask<K, V> produceTask) {
    getKafkaNativeProducer().send(new ProducerRecord<>(getTopic(produceTask), produceTask.getKey(),
        produceTask.getValue()), produceTask::markComplete);
  }

  public String getTopic(ProduceTask<K, V> produceTask) {
    if (StringUtils.isNotEmpty(produceTask.getTopic())) {
      return produceTask.getTopic();
    }
    return getDefaultTopic();
  }

  @Override
  public void batchProduce(List<ProduceTask<K, V>> produceTasks) {
    for (ProduceTask<K, V> task: produceTasks) {
      produce(task);
    }
  }

  @Override
  public void flush() {
    getKafkaNativeProducer().flush();
  }

  @Override
  public void close() {
    getKafkaNativeProducer().close();
  }
}

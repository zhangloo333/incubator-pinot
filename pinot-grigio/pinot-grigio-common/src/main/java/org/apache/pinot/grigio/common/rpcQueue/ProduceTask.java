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
package org.apache.pinot.grigio.common.rpcQueue;

import java.util.Objects;

/**
 * Class created to wrap around the kafka produce task object, so we can make the upsert (grigio) package stream independent
 * by using this implementation, we don't need to hard code kafka dependency in related package (pinot-server, pinot-core) etc
 */
public class ProduceTask<K, V> {

  private volatile boolean _completed = false;
  private Exception _exception = null;
  private Callback _callback = null;
  private String _topic;
  private K _key;
  private V _value;

  public ProduceTask(K key, V value) {
    this._key = key;
    this._value = value;
  }

  public ProduceTask(String topic, K key, V value) {
    this._topic = topic;
    this._key = key;
    this._value = value;
  }

  /**
   * topic might be null, if that is the case we assume this produce call will rely on default topic in producer
   * TODO: refactor this part such we ensure non-null return here
   * @return the name of the topic we are producing to, if there is such topic
   */
  public String getTopic() {
    return _topic;
  }

  public K getKey() {
    return _key;
  }

  public V getValue() {
    return _value;
  }

  public void setCallback(Callback callback) {
    _callback = callback;
  }

  public boolean isSucceed() {
    return this._completed && this._exception == null;
  }

  public Exception getException() {
    return this._exception;
  }

  /**
   * method to be called within native queue producer only, not supposed to be called by us
   * @param metadata the metadata associated with this call
   * @param exception any exception associated with this produce, null if no exception happened
   */
  public synchronized void markComplete(Object metadata, Exception exception) {
    if (!_completed) {
      _completed = true;
      _exception = exception;
      if (_callback != null) {
        if (exception == null) {
          _callback.onSuccess();
        } else {
          _callback.onFailure(exception);
        }
      }
    }
  }

  @Override
  public String toString() {
    return "ProduceTask{" +
        "_topic='" + _topic + '\'' +
        ", _key=" + _key +
        ", _value=" + _value.toString() +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ProduceTask<?, ?> that = (ProduceTask<?, ?>) o;
    return Objects.equals(_topic, that._topic) &&
        Objects.equals(_key, that._key) &&
        Objects.equals(_value, that._value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_topic, _key, _value);
  }

  public interface Callback {
    void onSuccess();

    void onFailure(Exception ex);
  }
}

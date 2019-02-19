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
package org.apache.pinot.common.data;

import java.util.List;
import org.apache.pinot.common.data.objects.JSONObject;
import org.apache.pinot.common.data.objects.MapObject;
import org.apache.pinot.common.data.objects.TextObject;
import org.apache.pinot.common.segment.fetcher.HdfsSegmentFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory class that create PinotObject from bytes
 */
public class PinotObjectFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotObjectFactory.class);

  public static PinotObject create(FieldSpec spec, byte[] buf) {
    return create(spec.getObjectType(), buf);
  }

  public static PinotObject create(String objectType, byte[] buf) {

    Class<? extends PinotObject> pinotObjectClazz;
    PinotObject pinotObject = null;
    try {
      switch (objectType.toUpperCase()) {
        case "MAP":
          pinotObjectClazz = MapObject.class;
          break;
        case "JSON":
          pinotObjectClazz = JSONObject.class;
          break;
        case "TEXT":
          pinotObjectClazz = TextObject.class;
          break;
        default:
          // custom object type.
          pinotObjectClazz = (Class<? extends PinotObject>) Class.forName(objectType);
      }
      pinotObject = pinotObjectClazz.getConstructor(new Class[]{}).newInstance(new Object[]{});
      pinotObject.init(buf);
    } catch (Exception e) {
      LOGGER.error("Error pinot object  for type:{}. Skipping inverted index creation", objectType);
    }
    return pinotObject;
  }
}

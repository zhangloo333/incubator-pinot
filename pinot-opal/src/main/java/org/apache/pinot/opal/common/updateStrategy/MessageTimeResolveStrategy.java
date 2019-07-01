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
package org.apache.pinot.opal.common.updateStrategy;

import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.opal.common.messages.KeyCoordinatorMessageContext;

public class MessageTimeResolveStrategy implements MessageResolveStrategy {
  @Override
  public boolean shouldDeleteFirstMessage(KeyCoordinatorMessageContext message1, KeyCoordinatorMessageContext message2) {
    if (message1.getTimestamp() < message2.getTimestamp()) {
      return true;
    } else if (message1.getTimestamp() > message2.getTimestamp()) {
      return false;
    } else {
      LLCSegmentName messageSegmentName1 = new LLCSegmentName(message1.getSegmentName());
      LLCSegmentName messageSegmentName2 = new LLCSegmentName(message2.getSegmentName());
      // if a message in the later segment, it should delete the same message belong to the earlier segment
      if (messageSegmentName1.getSequenceNumber() < messageSegmentName2.getSequenceNumber()) {
        return true;
      } else if (messageSegmentName1.getSequenceNumber() > messageSegmentName2.getSequenceNumber()) {
        return false;
      } else {
        // if both message in the same segment, the later message should delete the first message
        return message1.getKafkaOffset() < message2.getKafkaOffset();
      }
    }
  }
}

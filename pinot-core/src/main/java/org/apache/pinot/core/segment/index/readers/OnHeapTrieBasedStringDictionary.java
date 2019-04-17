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
package org.apache.pinot.core.segment.index.readers;

import com.hankcs.algorithm.AhoCorasickDoubleArrayTrie;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OnHeapTrieBasedStringDictionary extends OnHeapTrieBasedDictionary {
  private static final Logger LOGGER = LoggerFactory.getLogger(OnHeapTrieBasedStringDictionary.class);
  private final byte _paddingByte;
  private final String[] _unpaddedStrings;
  private final String[] _paddedStrings;
  private final AhoCorasickDoubleArrayTrie<Integer> _unpaddedAcdat;
  private final AhoCorasickDoubleArrayTrie<Integer> _paddedAcdat;

  public OnHeapTrieBasedStringDictionary(PinotDataBuffer dataBuffer, int length, int numBytesPerValue,
      byte paddingByte) throws IOException {
    super(dataBuffer, length, numBytesPerValue, paddingByte);

    _paddingByte = paddingByte;
    byte[] buffer = new byte[numBytesPerValue];
    _unpaddedStrings = new String[length];
    Map<String, Integer> unPaddedStringToIdMap = new HashMap<>(length);

    _unpaddedAcdat = new AhoCorasickDoubleArrayTrie<>();
    for (int i = 0; i < length; i++) {
      _unpaddedStrings[i] = getUnpaddedString(i, buffer);
      unPaddedStringToIdMap.put(_unpaddedStrings[i], i);
    }
    _unpaddedAcdat.build(unPaddedStringToIdMap);

    if (_paddingByte == 0) {
      _paddedStrings = null;
      _paddedAcdat = null;
    } else {
      _paddedStrings = new String[length];
      _paddedAcdat = new AhoCorasickDoubleArrayTrie<>();
      Map<String, Integer> paddedStringToIdMap = new HashMap<>(length);
      for (int i = 0; i < length; i++) {
        _paddedStrings[i] = getPaddedString(i, buffer);
        paddedStringToIdMap.put(_paddedStrings[i], i);
      }
      _paddedAcdat.build(paddedStringToIdMap);
    }
  }

  @Override
  public int indexOf(Object rawValue) {
    AhoCorasickDoubleArrayTrie<Integer> trie = (_paddingByte == 0) ? _unpaddedAcdat : _paddedAcdat;
    Integer index = trie.get((String) rawValue);
    return (index != null) ? index : -1;
  }

  @Override
  public int insertionIndexOf(Object rawValue) {
    return indexOf(rawValue);

  }

  @Override
  public String get(int dictId) {
    return _unpaddedStrings[dictId];
  }

  @Override
  public String getStringValue(int dictId) {
    return _unpaddedStrings[dictId];
  }
}

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
package org.apache.pinot.core.segment.virtualcolumn.mutable;

import org.apache.pinot.core.segment.index.ColumnMetadata;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;
import org.apache.pinot.core.segment.virtualcolumn.BaseVirtualColumnProvider;
import org.apache.pinot.core.segment.virtualcolumn.VirtualColumnContext;
import org.apache.pinot.spi.data.FieldSpec;

/**
 * base class for upsert related virtual column (validFrom and validUntil virtual column)
 * this only support the direct access through the reader but not the dictionary or inverted index
 */
public abstract class BaseLongVirtualColumnProvider extends BaseVirtualColumnProvider {

  @Override
  public Dictionary buildDictionary(VirtualColumnContext context) {
    return null;
  }

  @Override
  public ColumnMetadata buildMetadata(VirtualColumnContext context) {
    ColumnMetadata.Builder columnMetadataBuilder = new ColumnMetadata.Builder()
        .setCardinality(1) // TODO: double check on this
        .setHasDictionary(false)
        .setHasInvertedIndex(false)
        .setFieldType(FieldSpec.FieldType.DIMENSION)
        .setDataType(FieldSpec.DataType.LONG)
        .setSingleValue(true)
        .setIsSorted(false)
        .setTotalDocs(context.getTotalDocCount());
    return columnMetadataBuilder.build();
  }

  @Override
  public InvertedIndexReader buildInvertedIndex(VirtualColumnContext context) {
    return null;
  }
}

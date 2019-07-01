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
package org.apache.pinot.opal.common.StorageProvider;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Scanner;

public class UpdateLogStorageExplorer {
  public static void main(String[] args) throws IOException {
    Preconditions.checkState(args.length > 1, "need basepath as first parameter");
    String basePath = args[0];

    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(UpdateLogStorageProvider.BASE_PATH_CONF_KEY, basePath);

    UpdateLogStorageProvider.init(conf);
    UpdateLogStorageProvider provider = UpdateLogStorageProvider.getInstance();

    Scanner reader = new Scanner(System.in);
    System.out.println("please input the tablename and segment name to load");
    String input = reader.nextLine();
    String[] inputSplits = input.split(" ");
    Preconditions.checkState(inputSplits.length == 2, "expect input data to be 'tableName segmentName'");
    String tableName = inputSplits[0];
    String segmentName = inputSplits[0];

    List<UpdateLogEntry> updateLogEntryList = provider.getAllMessages(tableName, segmentName);
    Multimap<Long, UpdateLogEntry> map = ArrayListMultimap.create();
    System.out.println("update log size: " + updateLogEntryList.size());
    updateLogEntryList.forEach(u -> {
      map.put(u.getOffset(), u);
    });

    while (true) {
      System.out.println("input the offset");
      long offset = reader.nextLong();
      Collection<UpdateLogEntry> result = map.get(offset);
      System.out.println("associated update logs size: " + result.size());
      for (UpdateLogEntry entry: result) {
        System.out.println("content: " + entry.toString());
      }
    }
  }
}

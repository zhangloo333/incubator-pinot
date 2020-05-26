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
package org.apache.pinot.tools.admin.command;

import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.HybridQuickstart;
import org.apache.pinot.tools.Quickstart;
import org.apache.pinot.tools.RealtimeQuickStart;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QuickStartCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuickStartCommand.class.getName());

  @Option(name = "-type", required = false, metaVar = "<String>", usage = "Type of quickstart, supported: STREAM/BATCH/HYBRID")
  private String _type;

  @Option(name = "-tmpDir", required = false, aliases = {"-quickstartDir", "-dataDir"}, metaVar = "<String>", usage = "Temp Directory to host quickstart data")
  private String _tmpDir;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "QuickStart";
  }

  public QuickStartCommand setType(String type) {
    _type = type;
    return this;
  }

  public String getTmpDir() {
    return _tmpDir;
  }

  public void setTmpDir(String tmpDir) {
    _tmpDir = tmpDir;
  }

  @Override
  public String toString() {
    return ("QuickStart -type " + _type);
  }

  @Override
  public void cleanup() {

  }

  @Override
  public String description() {
    return "Run Pinot QuickStart.";
  }

  @Override
  public boolean execute()
      throws Exception {
    PluginManager.get().init();
    switch (_type.toUpperCase()) {
      case "OFFLINE":
      case "BATCH":
        Quickstart quickstart = new Quickstart();
        if (_tmpDir != null) {
          quickstart.setTmpDir(_tmpDir);
        }
        quickstart.execute();
        break;
      case "REALTIME":
      case "STREAM":
        RealtimeQuickStart realtimeQuickStart = new RealtimeQuickStart();
        if (_tmpDir != null) {
          realtimeQuickStart.setTmpDir(_tmpDir);
        }
        realtimeQuickStart.execute();
        break;
      case "HYBRID":
        HybridQuickstart hybridQuickstart = new HybridQuickstart();
        if (_tmpDir != null) {
          hybridQuickstart.setTmpDir(_tmpDir);
        }
        hybridQuickstart.execute();
        break;
      default:
        throw new UnsupportedOperationException("Unsupported QuickStart type: " + _type);
    }
    return true;
  }
}

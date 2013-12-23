/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.storm.yarn;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.io.Files;
import com.yahoo.storm.yarn.Client.ClientCommand;
import com.yahoo.storm.yarn.generated.StormMaster;

class StormTopologySubmitCommand implements ClientCommand {
  private static final Logger LOG = LoggerFactory
      .getLogger(StormMasterCommand.class);
  private String[] parameters;

  StormTopologySubmitCommand() {
  }

  @Override
  public Options getOpts() {
    Options opts = new Options();
    opts.addOption("appId", true, "(Required) The storm clusters app ID");
    opts.addOption("jar", true, "Storm jar file");
    opts.addOption("class", true, "Storm Topology class name");
    return opts;
  }
  
  @Override
  public String getHeaderDescription() {
    return "storm-yarn -appId=xx -jar=xx MainClass arg0 arg1 arg2";
  }

  @Override
  public void process(CommandLine cl) throws Exception {
    
    Map stormConf = Config.readStormConfig(null);
    
    String appId = cl.getOptionValue("appId");
    if (appId == null) {
      throw new IllegalArgumentException("-appId is required");
    }

    String jarName = cl.getOptionValue("jar");
    if (jarName == null) {
      throw new IllegalArgumentException("-appId is required");
    }

    String[] args = cl.getArgs();
    if (args.length <= 0) {
      throw new IllegalArgumentException("MainClass required. storm-yarn -appId=<id> -jar=<jar path> MainClass arg0 arg1...");
    }
    
    String className = args[0];
    this.parameters = new String[args.length - 1];
    for (int i = 1; i < args.length; i++) {
      this.parameters[i - 1] = args[i]; 
    }
    
    if (className == null) {
      throw new IllegalArgumentException("-appId is required");
    }

    StormOnYarn storm = null;
    File tmpStormConf = null;
    
    try {
      storm = StormOnYarn.attachToApp(appId, stormConf);
      StormMaster.Client client = storm.getClient();

      File tmpStormConfDir = Files.createTempDir();
      tmpStormConf = new File(tmpStormConfDir, "storm.yaml");
      StormMasterCommand.downloadStormYaml(client, tmpStormConf.getAbsolutePath());

      List<String> commands = Util.buildTopologySubmissionCommands(className,
          jarName, tmpStormConf.getAbsolutePath(), parameters);

      LOG.info("Running: " + Joiner.on(" ").join(commands));
      ProcessBuilder builder = new ProcessBuilder(commands);

      Process process = builder.start();
      Util.redirectStreamAsync(process.getInputStream(), System.out);
      Util.redirectStreamAsync(process.getErrorStream(), System.err);

      process.waitFor();
      
    } finally {
      if (storm != null) {
        storm.stop();
      }
      if (null != tmpStormConf) {
        tmpStormConf.delete();
      }
    }
  }
}

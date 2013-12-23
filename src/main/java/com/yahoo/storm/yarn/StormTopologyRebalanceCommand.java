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
import java.util.HashMap;
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

class StormTopologyRebalanceCommand implements ClientCommand {
  private static final Logger LOG = LoggerFactory
      .getLogger(StormMasterCommand.class);
  
  StormTopologyRebalanceCommand() {
  }

  @Override
  public Options getOpts() {
    Options opts = new Options();
    opts.addOption("appId", true, "(Required) The storm clusters app ID");
    opts.addOption("w", true, "(Optional) time(second) to wait");
    opts.addOption("n", true, "(Optional) new number of workers");
    opts.addOption("e", true, "(Optional) component:parallelism,component:parallelism,component:parallelism");
    return opts;
  }
  
  @Override
  public String getHeaderDescription() {
    return "storm-yarn rebalance -appId=<application id> -w=wait-time-secs -n=new-num-workers -e=component:parallelism,component:parallelism... topology-name";
  }

  @Override
  public void process(CommandLine cl) throws Exception {
    
    Map stormConf = Config.readStormConfig(null);
    
    String appId = cl.getOptionValue("appId");
    if (appId == null) {
      throw new IllegalArgumentException("-appId is required");
    }
    
    int secondsToWait = cl.getOptionValue("w") != null ? Integer.getInteger(cl.getOptionValue("w")) : -1;  
    int numWorkers = cl.getOptionValue("n") != null ? Integer.getInteger(cl.getOptionValue("n")) : -1;
    String parallelism = cl.getOptionValue("e");
    Map<String, Integer> parallelismMap = new HashMap<String, Integer>();
    if (null != parallelism) {
      String[] keyValues = parallelism.split(",");
      if (null != keyValues) {
        for (String kv : keyValues) {
          if (null != kv) {
            String [] keyAndValue = kv.split(":");
            if (keyAndValue.length == 2) {
              parallelismMap.put(keyAndValue[0], Integer.getInteger(keyAndValue[1]));
            }
          }
        }
      }
    }

    String[] args = cl.getArgs();
    if (args.length <= 0) {
      throw new IllegalArgumentException("tpologyId required. storm-yarn kill -appId=xx -w wait-time-seconds topologyId");
    }
    
    String topologyId = args[0];
    

    StormOnYarn storm = null;
    File tmpStormConf = null;
    
    try {
      storm = StormOnYarn.attachToApp(appId, stormConf);
      StormMaster.Client client = storm.getClient();

      File tmpStormConfDir = Files.createTempDir();
      tmpStormConf = new File(tmpStormConfDir, "storm.yaml");
      StormMasterCommand.downloadStormYaml(client, tmpStormConf.getAbsolutePath());

      List<String> commands = Util.buildRebalanceCommands(tmpStormConf.getAbsolutePath(), topologyId, 
          secondsToWait, numWorkers, parallelismMap);

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

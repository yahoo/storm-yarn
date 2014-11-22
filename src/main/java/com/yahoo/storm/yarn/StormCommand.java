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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.io.Files;
import com.yahoo.storm.yarn.Client.ClientCommand;
import com.yahoo.storm.yarn.generated.StormMaster;

/**
 * Works as Storm shell command
 * 
 */
public abstract class StormCommand implements ClientCommand {

  private static final Logger LOG = LoggerFactory.getLogger(StormCommand.class);

  protected String appId;

  @Override
  public Options getOpts() {
    Options opts = new Options();
    opts.addOption("appId", true, "(Required) The storm clusters app ID");
    return opts;
  }

  protected void process(String command, CommandLine cl) throws Exception {
    this.appId = cl.getOptionValue("appId");

    if (appId == null) {
      throw new IllegalArgumentException("-appId is required");
    }

    Map stormConf = Config.readStormConfig(null);

    StormOnYarn storm = null;

    try {

      storm = StormOnYarn.attachToApp(appId, stormConf);
      StormMaster.Client client = storm.getClient();

      File tmpStormConfDir = Files.createTempDir();
      File tmpStormConf = new File(tmpStormConfDir, "storm.yaml");

      StormMasterCommand.downloadStormYaml(client,
          tmpStormConf.getAbsolutePath());

      String stormHome = System.getProperty("storm.home");
      
      List<String> commands = new ArrayList<String>();
      commands.add("python"); // TODO: Change this to python home
      commands.add(stormHome + "/bin/storm");
      commands.add(command);
      
      String[] args = cl.getArgs();
      if (null != args && args.length > 0) {
        for (int i = 0; i < args.length; i++) {
          commands.add(args[i]);
        }
      }
      commands.add("--config");
      commands.add(tmpStormConf.getAbsolutePath());

      LOG.info("Running: " + Joiner.on(" ").join(commands));
      ProcessBuilder builder = new ProcessBuilder(commands);

      Process process = builder.start();
      Util.redirectStreamAsync(process.getInputStream(), System.out);
      Util.redirectStreamAsync(process.getErrorStream(), System.err);

      process.waitFor();

      if (process.exitValue() == 0) {
        if (null != tmpStormConfDir) {
          tmpStormConf.delete();
        }
      }
    } finally {
      if (storm != null) {
        storm.stop();
      }
    }
  }
}

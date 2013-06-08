/*
 * Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
 *
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

import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.yahoo.storm.yarn.Client.ClientCommand;

public class LaunchCommand implements ClientCommand {
  @Override
  public Options getOpts() {
    Options opts = new Options();
    opts.addOption("appname", true,
        "Application Name. Default value - Storm-on-Yarn");
    opts.addOption("queue", true,
        "RM Queue in which this application is to be submitted");
    return opts;
  }

  @Override
  public void process(CommandLine cl, 
      @SuppressWarnings("rawtypes") Map stormConf) throws Exception {
    String appName = cl.getOptionValue("appname", "Storm-on-Yarn");
    String queue = cl.getOptionValue("queue", "default");
    Integer amSize = (Integer) stormConf.get(Config.MASTER_SIZE_MB);
    if (amSize == null) {
      //TODO we should probably have a good guess here, but for now
      amSize = 4096; //4GB
    }
    
    StormOnYarn storm = null;
    try {
      storm = StormOnYarn.launchApplication(appName, queue, amSize, stormConf);
      System.err.println("Submitted application " + storm.getAppId());
    } finally {
      if (storm != null) {
        storm.stop();
      }
    }
  }
}

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

public class StopNimbusCommand implements ClientCommand {

  @Override
  public Options getOpts() {
    Options opts = new Options();
    //TODO can we make this required
    opts.addOption("appId", true, "The storm clusters app ID");
    return opts;
  }

  @Override
    public void process(CommandLine cl,
            @SuppressWarnings("rawtypes") Map stormConf) throws Exception {
    String appId = cl.getOptionValue("appId");
    if(appId == null) {
      throw new IllegalArgumentException("-appId is required");
    }
    
    StormOnYarn storm = null;
    try {
      storm = StormOnYarn.attachToApp(appId, stormConf);
      storm.stopNimbus();
    } finally {
      if (storm != null) {
        storm.stop();
      }
    }
  }

}

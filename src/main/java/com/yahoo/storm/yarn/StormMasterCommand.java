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

import java.io.PrintStream;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.thrift7.transport.TTransportException;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.storm.yarn.Client.ClientCommand;
import com.yahoo.storm.yarn.generated.StormMaster;

public class StormMasterCommand implements ClientCommand {
    private static final Logger LOG = LoggerFactory.getLogger(StormMasterCommand.class);
    enum COMMAND {
        GET_STORM_CONFIG,  
        SET_STORM_CONFIG,  
        START_NIMBUS, 
        STOP_NIMBUS, 
        START_UI, 
        STOP_UI, 
        ADD_SUPERVISORS,
        START_SUPERVISORS,
        STOP_SUPERVISORS 
    };
    COMMAND cmd;

    StormMasterCommand(COMMAND cmd) {
        this.cmd = cmd;
    }

    @Override
    public Options getOpts() {
        Options opts = new Options();
        //TODO can we make this required
        opts.addOption("appId", true, "(Required) The storm clusters app ID");

        opts.addOption("output", true, "Output file");
        opts.addOption("supversiors", true, "(Required for addSupervisors) The # of supervisors to be added");
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
        String conf_str = null;
        try {
            storm = StormOnYarn.attachToApp(appId, stormConf);
            StormMaster.Client client = storm.getClient();
            switch (cmd) {
            case GET_STORM_CONFIG:
                try { 
                    conf_str = client.getStormConf();                  
                } catch (TTransportException ex) {
                    LOG.info(ex.toString());
                }
                if (conf_str != null) {
                    String output = cl.getOptionValue("output");
                    PrintStream os = (output!=null? new PrintStream(output) : System.out);
                    os.println(conf_str);
                    if (output != null) os.close();
                }
                break;

            case SET_STORM_CONFIG:
                String storm_conf_str = JSONValue.toJSONString(stormConf);
                try { 
                    client.setStormConf(storm_conf_str);  
                } catch (TTransportException ex) {
                    LOG.info(ex.toString());
                }
                break;

            case ADD_SUPERVISORS:
                String supversiors = cl.getOptionValue("supversiors", "1");
                try {
                    client.addSupervisors(new Integer(supversiors).intValue());  
                } catch (TTransportException ex) {
                    LOG.info(ex.toString());
                }
                break;

            case START_NIMBUS:
                try {
                    client.startNimbus();
                } catch (TTransportException ex) {
                    LOG.info(ex.toString());
                }
                break;

            case STOP_NIMBUS:
                try {
                    client.stopNimbus();
                } catch (TTransportException ex) {
                    LOG.info(ex.toString());
                }
                break;

            case START_UI:
                try {
                    client.startUI();
                } catch (TTransportException ex) {
                    LOG.info(ex.toString());
                }
                break;

            case STOP_UI:
                try {
                    client.stopUI();
                } catch (TTransportException ex) {
                    LOG.info(ex.toString());
                }
                break;

            case START_SUPERVISORS:
                try {
                    client.startSupervisors();
                } catch (TTransportException ex) {
                    LOG.info(ex.toString());
                }
                break;

            case STOP_SUPERVISORS:
                try {
                    client.stopSupervisors();
                } catch (TTransportException ex) {
                    LOG.info(ex.toString());
                }
                break;
            }
        } finally {
            if (storm != null) {
                storm.stop();
            }
        }
    }
}

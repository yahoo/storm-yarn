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

import com.yahoo.storm.yarn.generated.StormMaster;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.transport.TTransportException;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileWriter;
import java.util.List;
import java.util.Map;

class StormMasterCommand implements Client.ClientCommand {
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
        STOP_SUPERVISORS,
        SHUTDOWN,
        REMOVE_SUPERVISORS
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
        opts.addOption("supervisors", true, "(Required for addSupervisors) The # of supervisors to be added");
        opts.addOption("supervisor", true, "(Required for removeSupervisors) the supervisor to be remove");

        return opts;
    }
    
    @Override
    public String getHeaderDescription() {
      return null;
    }

    @Override
    public void process(CommandLine cl) throws Exception {
      
        String config_file = null;
        List remaining_args = cl.getArgList();
        if (remaining_args!=null && !remaining_args.isEmpty()) {
            config_file = (String)remaining_args.get(0);
        }
        Map stormConf = Config.readStormConfig(null);
      
        String appId = cl.getOptionValue("appId");
        if(appId == null) {
            throw new IllegalArgumentException("-appId is required");
        }

        StormOnYarn storm = null;
        try {
            storm = StormOnYarn.attachToApp(appId, stormConf);
            StormMaster.Client client = storm.getClient();
            switch (cmd) {
            case REMOVE_SUPERVISORS:
                String supversior = cl.getOptionValue("supervisor");
                try {
                    client.removeSupervisors(supversior);
                } catch (TTransportException ex) {
                    LOG.info(ex.toString());
                }
                break;
            case GET_STORM_CONFIG:
                downloadStormYaml(client, cl.getOptionValue("output"));
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
                String supversiors = cl.getOptionValue("supervisors", "1");
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

            case SHUTDOWN:
                try {
                    client.shutdown();
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

    public static void downloadStormYaml(StormMaster.Client client, String storm_yaml_output) {
        String  conf_str = "Not Avaialble";

        //fetch storm.yaml from Master
        try {
            conf_str = client.getStormConf();
        } catch (TTransportException ex) {
            LOG.error("Exception in downloading storm.yaml", ex);
        } catch (TException ex) {
            LOG.error("Exception in downloading storm.yaml", ex);
        }

        //storm the fetched storm.yaml into storm_yaml_output or stdout
        try {
            Object json = JSONValue.parse(conf_str);
            Map<?, ?> conf = (Map<?, ?>)json;
            Yaml yaml = new Yaml();

            if (storm_yaml_output == null) {
                LOG.info("storm.yaml downloaded:");
                System.out.println(yaml.dump(conf));
            } else {
                FileWriter out = new FileWriter(storm_yaml_output);
                yaml.dump(conf, out);
                out.flush();
                out.close();
                LOG.info("storm.yaml downloaded into "+storm_yaml_output);
            }
        } catch (Exception ex) {
            LOG.error("Exception in storing storm.yaml. ", ex);
        }
    }
}

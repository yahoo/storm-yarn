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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import backtype.storm.utils.Utils;

public class Config {
    final public static String MASTER_DEFAULTS_CONFIG = "master_defaults.yaml";
    final public static String MASTER_CONFIG = "master.yaml";
    final public static String MASTER_HOST = "master.host";
    final public static String MASTER_THRIFT_PORT = "master.thrift.port";
    final public static String MASTER_TIMEOUT_SECS = "master.timeout.secs";
    final public static String MASTER_SIZE_MB = "master.container.size-mb";
    final public static String MASTER_NUM_SUPERVISORS = "master.initial-num-supervisors";
    final public static String MASTER_CONTAINER_PRIORITY = "master.container.priority";
    final public static String MASTER_HEARTBEAT_INTERVAL_MILLIS = "master.heartbeat.interval.millis";
    
    @SuppressWarnings("rawtypes")
    static public Map readStormConfig() {
        return readStormConfig(null);
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    static Map readStormConfig(String stormYarnConfigPath) {
        //default configurations
        Map ret = Utils.readDefaultConfig();
        Map conf = Utils.findAndReadConfigFile(Config.MASTER_DEFAULTS_CONFIG);
        ret.putAll(conf);
        
        //standard storm configuration
        String confFile = System.getProperty("storm.conf.file");
        Map storm_conf;
        if (confFile==null || confFile.equals("")) {
            storm_conf = Utils.findAndReadConfigFile("storm.yaml", false);
        } else {
            storm_conf = Utils.findAndReadConfigFile(confFile, true);
        }
        ret.putAll(storm_conf);
        
        //master configuration file 
        if (stormYarnConfigPath == null) {
            Map master_conf = Utils.findAndReadConfigFile(Config.MASTER_CONFIG, false);
            ret.putAll(master_conf);
        }
        else {
            try {
                Yaml yaml = new Yaml();
                FileInputStream is = new FileInputStream(stormYarnConfigPath);
                Map storm_yarn_config = (Map) yaml.load(is);
                if(storm_yarn_config!=null)
                    ret.putAll(storm_yarn_config);
                is.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        //other configuration settings via CLS opts per system property: storm.options
        ret.putAll(Utils.readCommandLineOpts());

        return ret;
    }

}

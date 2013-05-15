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

    static public Map readStormConfig() {
        Map conf = Utils.findAndReadConfigFile(Config.MASTER_DEFAULTS_CONFIG);
        conf.putAll(Utils.readStormConfig());
        Map master_conf = Utils.findAndReadConfigFile(Config.MASTER_CONFIG, false);
        if (master_conf != null) conf.putAll(master_conf);
        return conf;
    }
}

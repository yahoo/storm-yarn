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

import org.apache.thrift7.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.security.auth.ThriftClient;
import backtype.storm.utils.Utils;

import com.yahoo.storm.yarn.generated.StormMaster;

public class MasterClient extends ThriftClient {
    private StormMaster.Client _client;
    private static final Logger LOG = LoggerFactory.getLogger(MasterClient.class);

    public static MasterClient getConfiguredClient(Map conf) {
        try {
            String masterHost = (String) conf.get(Config.MASTER_HOST);
            int masterPort = Utils.getInt(conf.get(Config.MASTER_THRIFT_PORT));
            try {
            	Integer timeout = Utils.getInt(conf.get(Config.MASTER_TIMEOUT_SECS));
            	return new MasterClient(conf, masterHost, masterPort, timeout);
            } catch (IllegalArgumentException e) {
            	return new MasterClient(conf, masterHost, masterPort, null);
            }
            
        } catch (TTransportException ex) {
            throw new RuntimeException(ex);
        }
    }

    public MasterClient(Map conf, String host, int port, Integer timeout) throws TTransportException {
        super(conf, host, port, timeout);
        _client = new StormMaster.Client(_protocol);
    }

    public StormMaster.Client getClient() {
        return _client;
    }
}

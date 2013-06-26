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

import java.io.IOException;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.yahoo.storm.yarn.generated.StormMaster;

public class TestStormMaster {

    private static final Log LOG = LogFactory.getLog(TestStormMaster.class);
    private static EmbeddedZKServer zkServer;
    private static MasterServer server = null;
    private static MasterClient client = null;
    private static TestConfig testConf = new TestConfig(); 

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @BeforeClass
    public static void setup() throws InterruptedException, IOException {
        //start embedded ZK server
        zkServer = new EmbeddedZKServer();
        zkServer.start();

        //simple configuration
        final Map storm_conf = Config.readStormConfig("src/main/resources/master_defaults.yaml");
        storm_conf.put(backtype.storm.Config.STORM_ZOOKEEPER_PORT, zkServer.port());
        
        String storm_home = testConf.stormHomePath();
        if (storm_home == null) {
            throw new RuntimeException("Storm home was not found."
                    + "  Make sure to include storm in the PATH.");
        }
        LOG.info("Will be using storm found on PATH at "+storm_home);

        final YarnConfiguration hadoopConf = new YarnConfiguration();
        ApplicationAttemptId appAttemptId = Records.newRecord(ApplicationAttemptId.class);

        StormAMRMClient client = new StormAMRMClient(appAttemptId, storm_conf, hadoopConf);

        LOG.info("Storm server attaching to port: "+ storm_conf.get(Config.MASTER_THRIFT_PORT));
        //launch server
        server = new MasterServer(storm_conf, client);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    server.serve();
                } catch (Exception e) {
                    LOG.error(e);
                }
            }
        }).start();
        while (!server.isServing()) 
            Thread.sleep(10);
        LOG.info("Storm server started at port: "+ storm_conf.get(Config.MASTER_THRIFT_PORT));

        //launch client
        TestStormMaster.client = MasterClient.getConfiguredClient(storm_conf);
    }

    @AfterClass
    public static void tearDown() throws IOException {
        //stop client
        if (client != null) {
            client.close();
            client = null;
        }

        //stop server
        if (server != null) {
            server.stop();
            server = null;
        }

        //remove configuration file
        testConf.cleanup();
        
        //shutdown Zookeeper server
        if (zkServer != null) {
            zkServer.stop();
            zkServer = null;
        }
    }

    @Test
    public void test1() throws Exception {
        StormMaster.Client master_client = client.getClient();

        LOG.info("getStormConf");
        Assert.assertNotNull(master_client.getStormConf());

        LOG.info("addSupervisors(1)");
        master_client.addSupervisors(1); 
    }
}

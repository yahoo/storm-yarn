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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;

import junit.framework.Assert;

import org.apache.thrift7.TException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.Mockito.mock;

import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import com.yahoo.storm.yarn.generated.StormMaster;

public class TestStormCluster {
    static final Logger LOG = LoggerFactory.getLogger(TestStormMaster.class);
    
    protected static MasterServer server = null;
    protected static MasterClient client = null;
    
    @BeforeClass
    public static void setup() throws InterruptedException, IOException {

        String storm_home = getStormHomePath();
        if (storm_home == null) {
            throw new RuntimeException("Storm home was not found."
                    + "  Make sure to include storm in the PATH.");
        }
        LOG.info("Will be using storm found on PATH at "+storm_home);
        System.setProperty("storm.home", storm_home);

        //simple configuration
        @SuppressWarnings("rawtypes")
        final Map storm_conf = Config.readStormConfig(null);
        LOG.info("Storm server attaching to port: "+ storm_conf.get(Config.MASTER_THRIFT_PORT));
        StormAMRMClient mockClient = mock(StormAMRMClient.class);
        server = new MasterServer(storm_conf, mockClient);

        confirmNothingIsRunning(storm_conf);

        //launch server
        new Thread(new Runnable() {
            @Override
            public void run() {
                server.serve();
            }
        }).start();

        LOG.info("Sleep to wait for the server to startup");
        final int timeoutSecs = 40;
        for (int elapsedSecs=0; elapsedSecs < timeoutSecs; elapsedSecs++) {
            Thread.sleep(1000);
            LOG.info("Slept " + elapsedSecs + " of " + timeoutSecs + "s.");
            try {
                checkZkConnection(storm_conf);
            } catch (IOException e) {
                LOG.warn("Could not connect to zookeeper server");
                continue;
            }
            try {
                checkNimbusConnection(storm_conf);
            } catch (IOException e) {
                LOG.warn("Still cannot connect to nimbus server.");
                continue;
            }

            // The server appears to be up.  Launch the client.
            client = MasterClient.getConfiguredClient(storm_conf);
            LOG.info("Connected to master to get client");

            return;
        }

        throw new RuntimeException("Failed to connect to nimbus server in "
                + timeoutSecs + "seconds.");
    }

    private static void checkNimbusConnection(
            @SuppressWarnings("rawtypes") final Map storm_conf) 
            throws IOException, UnknownHostException {
        // Try to open a TCP connection to the nimbus port.
        new Socket((String) storm_conf.get(Config.MASTER_HOST),
                (Integer) storm_conf
                        .get(backtype.storm.Config.NIMBUS_THRIFT_PORT))
                .close();
    }

    private static void checkZkConnection(
            @SuppressWarnings("rawtypes") final Map storm_conf)
            throws IOException, UnknownHostException {
        // Try to open a TCP connection to the zookeeper ports
        new Socket("localhost", 
                (Integer) storm_conf
                    .get(backtype.storm.Config.STORM_ZOOKEEPER_PORT))
                .close();
    }

    private static void checkUiConnection(Map<?, ?> storm_conf)
            throws IOException, UnknownHostException {
        // Try to open a TCP connection to the UI port.
        new Socket((String) storm_conf.get(Config.MASTER_HOST),
                (Integer) storm_conf
                        .get(backtype.storm.Config.UI_PORT))
                .close();
        
    }

    private static void confirmNothingIsRunning(Map<?, ?> storm_conf) {
        try {
            checkNimbusConnection(storm_conf);
            throw new RuntimeException("Nimbus server already running.");
        } catch (IOException e) {
            LOG.info("OK: Nimbus does not seem to be running.");
        }
        
        try {
            checkUiConnection(storm_conf);
            throw new RuntimeException("UI server already running.");
        } catch (IOException e) {
            LOG.info("OK: UI does not seem to be running.");
        }
        
    }

    private static String getStormHomePath() throws IOException {
        String pathEnvString = System.getenv().get("PATH");
        for (String pathStr : pathEnvString.split(File.pathSeparator)) {
            // Is storm binary located here?  Path start = fs.getPath(pathStr);
            File f = new File(pathStr);
            if (f.isDirectory()) {
                File[] files = f.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        if (name.equals("storm")) {
                            return true;
                        }
                        return false;
                    }
                });
                if (files.length > 0) {
                    File canonicalPath = new File(pathStr + File.separator +
                            "storm").getCanonicalFile();
                    return (canonicalPath.getParentFile().getParent());
                }
            }
        }
        return null;
    }

    @AfterClass
    public static void tearDown() throws IOException {
        //stop client
        if (client != null) {
            StormMaster.Client master_client = client.getClient();
            try {
                master_client.stopNimbus();
                master_client.stopUI();
            } catch (TException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            client.close();
            client = null;
        }

        //stop server
        if (server != null) {
            server.stop();
            server = null;
        }
    }

    @Test
    public void testUI() throws Exception {
        LOG.info("Testing UI");
        @SuppressWarnings("rawtypes")
        final Map storm_conf = Config.readStormConfig(null);
        LOG.info("Testing connection to UI ...");
        URL url = new URL("http://"+storm_conf.get("ui.host")+":"+storm_conf.get("ui.port")+"/");
        URLConnection con = url.openConnection();
        Assert.assertNotNull(con.getContent());
    }
}

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.thrift7.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.NimbusClient;

import com.google.common.base.Joiner;

public class TestIntegration {
    private static final Logger LOG = LoggerFactory.getLogger(TestIntegration.class);
    private static MiniYARNCluster yarnCluster = null;
    private static String appId;
    private static EmbeddedZKServer zkServer;
    private static File storm_conf_file;
    private static File yarn_site_xml;
    private static String storm_home;
    private static TestConfig testConf = new TestConfig(); 
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @BeforeClass
    public static void setup() {
        try {
            zkServer = new EmbeddedZKServer();
            zkServer.start();
            
            LOG.info("Starting up MiniYARN cluster");
            if (yarnCluster == null) {
                yarnCluster = new MiniYARNCluster(TestIntegration.class.getName(), 1, 1, 1);
                Configuration conf = new YarnConfiguration();
                conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
                yarnCluster.init(conf);
                yarnCluster.start();
            }
            
            sleep(2000);
            
            Configuration miniyarn_conf = yarnCluster.getConfig();
            yarn_site_xml = testConf.createYarnSiteConfig(miniyarn_conf);
            
            storm_home = testConf.stormHomePath();
            LOG.info("Will be using storm found on PATH at "+storm_home);

            //create a storm configuration file with zkport 
            final Map storm_conf = Config.readStormConfig();
            storm_conf.put(backtype.storm.Config.STORM_ZOOKEEPER_PORT, zkServer.port());
            storm_conf.put(Config.MASTER_HEARTBEAT_INTERVAL_MILLIS, 100);
            storm_conf_file = testConf.createConfigFile(storm_conf);

            List<String> cmd = java.util.Arrays.asList("bin/storm-yarn",
                    "launch",
                    storm_conf_file.toString(),
                    "--stormZip",
                    "lib/storm.zip",
                    "--appname",
                    "storm-on-yarn-test",
                    "--output",
                    "target/appId.txt");
            int status = execute(cmd);

            //wait for Storm cluster to be fully luanched
            sleep(30000); 

            BufferedReader reader = new BufferedReader(new FileReader ("target/appId.txt"));
            appId = reader.readLine();
            reader.close();
            if (appId!=null) appId = appId.trim();
            LOG.info("application ID:"+appId);
        } catch (Exception ex) {
            Assert.assertEquals(null, ex);
            LOG.error("setup failure", ex);
        }
    }

    private static void sleep(int i) {
        try {
            Thread.sleep(i);
        } catch (InterruptedException e) {
            LOG.info("setup thread sleep interrupted. message=" + e.getMessage());
        }   
    }

    @Test
    public void performActions() throws Exception { 
        try {
            List<String> cmd = java.util.Arrays.asList("bin/storm-yarn",
                    "getStormConfig",
                    storm_conf_file.toString(),
                    "--appId",
                    appId,
                    "--output",
                    storm_home+"/storm.yaml");
            execute(cmd);
            sleep(1000);

            cmd = java.util.Arrays.asList("bin/storm-yarn",
                    "addSupervisors",
                    storm_conf_file.toString(),
                    "--supervisors",
                    "2",
                    "--appId",
                    appId);
            execute(cmd);
            sleep(1000);

            cmd = java.util.Arrays.asList(storm_home+"/bin/storm",
                    "jar",
                    "lib/storm-starter-0.0.1-SNAPSHOT.jar",
                    "storm.starter.WordCountTopology", 
                    "word-count-topology");
            execute(cmd);
            sleep(5000);

            Map storm_conf = Config.readStormConfig(storm_home+"/storm.yaml");
            Nimbus.Client nimbus_client = NimbusClient.getConfiguredClient(storm_conf).getClient();
            ClusterSummary cluster_summary = nimbus_client.getClusterInfo();
            TopologySummary topology_summary = cluster_summary.get_topologies().get(0);
            Assert.assertEquals("ACTIVE", topology_summary.get_status());
            
            cmd = java.util.Arrays.asList("bin/storm-yarn",
                    "stopNimbus",
                    storm_conf_file.toString(),
                    "--appId",
                    appId);
            execute(cmd);
            sleep(1000);

            cmd = java.util.Arrays.asList("bin/storm-yarn",
                    "startNimbus",
                    storm_conf_file.toString(),
                    "--appId",
                    appId);
            execute(cmd);
            sleep(1000);

            cmd = java.util.Arrays.asList("bin/storm-yarn",
                    "stopUI",
                    storm_conf_file.toString(),
                    "--appId",
                    appId);
            execute(cmd);
            sleep(1000);

            cmd = java.util.Arrays.asList("bin/storm-yarn",
                    "startUI",
                    storm_conf_file.toString(),
                    "--appId",
                    appId);
            execute(cmd);
            sleep(1000);
        } catch (Exception ex) {
            Assert.assertEquals(null, ex);
        }
    }

    @AfterClass
    public static void tearDown() throws IOException, TException {        
        //shutdown Storm Cluster
        try {
            List<String> cmd = java.util.Arrays.asList("bin/storm-yarn",
                    "shutdown",
                    storm_conf_file.toString(),
                    "--appId",
                    appId);
            execute(cmd);
            sleep(1000);
        } catch (Exception ex) {
            Assert.assertEquals(null, ex);
        }
        
        //shutdown YARN cluster
        if (yarnCluster != null) {
            LOG.info("shutdown MiniYarn cluster");
            yarnCluster.stop();
            yarnCluster = null;
        }
        sleep(1000);
        
        //remove configuration file
        testConf.cleanup();

        //shutdown Zookeeper server
        if (zkServer != null) {
            zkServer.stop();
            zkServer = null;
        }
   }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static int execute(List<String> cmd) throws InterruptedException, IOException {
        LOG.info(Joiner.on(" ").join(cmd));           
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectError(Redirect.INHERIT).redirectOutput(Redirect.INHERIT);
        pb.redirectErrorStream(true);
        pb.redirectOutput();
        Map env = pb.environment();
        env.putAll(System.getenv());
        env.put(Environment.PATH.name(), "./bin:"+storm_home+"/bin:"+env.get(Environment.PATH.name()));
        env.put("STORM_YARN_CONF_DIR", yarn_site_xml.getParent().toString());
        Process proc = pb.start();
        int status = proc.waitFor();
        return status;
    }
}

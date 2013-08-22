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
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
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
import backtype.storm.utils.Utils;

import com.google.common.base.Joiner;

public class TestIntegration {
    private static final Logger LOG = LoggerFactory.getLogger(TestIntegration.class);
    private static final String STORM_YARN_CMD = "bin"+File.separator+"storm-yarn";
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
                yarnCluster = new MiniYARNCluster(TestIntegration.class.getName(), 2, 1, 1);
                Configuration conf = new YarnConfiguration();
                conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
                conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 2*1024);
                conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_CORES, 1);
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
            execute(cmd);

            //wait for Storm cluster to be fully luanched
            sleep(15000); 

            BufferedReader reader = new BufferedReader(new FileReader ("target/appId.txt"));
            appId = reader.readLine();
            reader.close();
            if (appId!=null) appId = appId.trim();
            LOG.info("application ID:"+appId);
        } catch (Exception ex) {
            LOG.error("setup failure", ex);
            Assert.assertEquals(null, ex);
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
            List<String> cmd = java.util.Arrays.asList(STORM_YARN_CMD,
                    "getStormConfig",
                    storm_conf_file.toString(),
                    "--appId",
                    appId,
                    "--output",
                    storm_home+"/storm.yaml");
            execute(cmd);
            sleep(1000);

            cmd = java.util.Arrays.asList(storm_home+"/bin/storm",
                    "jar",
                    "lib/storm-starter-0.0.1-SNAPSHOT.jar",
                    "storm.starter.ExclamationTopology", 
                    "exclamation-topology");
            execute(cmd);
            sleep(30000);

            if (new File(storm_home+"/storm.yaml").exists()) {
                Map storm_conf = Config.readStormConfig(storm_home+"/storm.yaml");
                Nimbus.Client nimbus_client = NimbusClient.getConfiguredClient(storm_conf).getClient();
                ClusterSummary cluster_summary = nimbus_client.getClusterInfo();
                TopologySummary topology_summary = cluster_summary.get_topologies().get(0);
                Assert.assertEquals("ACTIVE", topology_summary.get_status());
            }

            cmd = java.util.Arrays.asList(storm_home+"/bin/storm",
                    "kill",
                    "exclamation-topology");
            execute(cmd);
            sleep(1000);

            cmd = java.util.Arrays.asList(STORM_YARN_CMD,
                    "stopNimbus",
                    storm_conf_file.toString(),
                    "--appId",
                    appId);
            execute(cmd);
            sleep(1000);

            cmd = java.util.Arrays.asList(STORM_YARN_CMD,
                    "startNimbus",
                    storm_conf_file.toString(),
                    "--appId",
                    appId);
            execute(cmd);
            sleep(1000);

            cmd = java.util.Arrays.asList(STORM_YARN_CMD,
                    "stopUI",
                    storm_conf_file.toString(),
                    "--appId",
                    appId);
            execute(cmd);
            sleep(1000);

            cmd = java.util.Arrays.asList(STORM_YARN_CMD,
                    "startUI",
                    storm_conf_file.toString(),
                    "--appId",
                    appId);
            execute(cmd);
            sleep(1000);
        } catch (Exception ex) {
            LOG.warn("Exception in Integration Test", ex);
            Assert.assertEquals(null, ex);
        }
    }

    @AfterClass
    public static void tearDown() {        
        try {
            //shutdown Storm Cluster
            List<String> cmd = java.util.Arrays.asList(STORM_YARN_CMD,
                    "shutdown",
                    storm_conf_file.toString(),
                    "--appId",
                    appId);
            execute(cmd);
            sleep(1000);
        } catch (Exception ex) {
            LOG.info(ex.toString());
        }

        //shutdown Zookeeper server
        if (zkServer != null) {
            zkServer.stop();
            zkServer = null;
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
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static int execute(List<String> cmd) throws InterruptedException, IOException {
        LOG.info(Joiner.on(" ").join(cmd));           
        ProcessBuilder pb = new ProcessBuilder(cmd);
        Map env = pb.environment();
        env.putAll(System.getenv());
        env.put(Environment.PATH.name(), "bin:"+storm_home+File.separator+"bin:"+env.get(Environment.PATH.name()));
        String yarn_conf_dir = yarn_site_xml.getParent().toString();
        env.put("STORM_YARN_CONF_DIR", yarn_conf_dir);        
        List<URL> logback_xmls = Utils.findResources("logback.xml");
        if (logback_xmls != null && logback_xmls.size()>=1) {
            String logback_xml = logback_xmls.get(0).getFile();
            LOG.debug("logback_xml:"+yarn_conf_dir+File.separator+"logback.xml");
            FileUtils.copyFile(new File(logback_xml), new File(yarn_conf_dir+File.separator+"logback.xml"));
        }
        List<URL> log4j_properties = Utils.findResources("log4j.properties");
        if (log4j_properties != null && log4j_properties.size()>=1) {
            String log4j_properties_file = log4j_properties.get(0).getFile();
            LOG.debug("log4j_properties_file:"+yarn_conf_dir+File.separator+"log4j.properties");
            FileUtils.copyFile(new File(log4j_properties_file), new File(yarn_conf_dir+File.separator+"log4j.properties"));
        }

        Process proc = pb.start();
        Util.redirectStreamAsync(proc.getInputStream(), System.out);
        Util.redirectStreamAsync(proc.getErrorStream(), System.err);
        int status = proc.waitFor();
        return status;
    }
}

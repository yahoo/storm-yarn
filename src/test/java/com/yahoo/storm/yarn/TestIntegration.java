package com.yahoo.storm.yarn;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.thrift7.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.storm.yarn.StormOnYarn;
import com.yahoo.storm.yarn.generated.StormMaster;

public class TestIntegration {
    static final Logger LOG = LoggerFactory.getLogger(TestIntegration.class);
    static MiniYARNCluster yarnCluster = null;
    static Configuration conf = new YarnConfiguration();
    static String rmAddr;
    static String appId;
    
    @BeforeClass
    public static void setup() {
       try {
           LOG.info("Starting up MiniYARN cluster");
           if (yarnCluster == null) {
               yarnCluster = new MiniYARNCluster(TestIntegration.class.getName(), 1, 1, 1);
               conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
               yarnCluster.init(conf);
               yarnCluster.start();
           }
           try {
               Thread.sleep(2000);
               rmAddr = yarnCluster.getConfig().get(YarnConfiguration.RM_ADDRESS);
           } catch (InterruptedException e) {
               LOG.info("setup thread sleep interrupted. message=" + e.getMessage());
           }   

           String cmd = "bin/storm-yarn launch src/main/resources/master_defaults.yaml --appname storm-on-yarn-test --output target/appId --rmAddr "+rmAddr;
           LOG.info("Launch: "+cmd);           
           Runtime.getRuntime().exec(cmd);
           
           BufferedReader reader = new BufferedReader( new FileReader ("target/appId"));
           appId = reader.readLine();
           if (appId!=null) appId = appId.trim();
           LOG.info("application ID:"+appId);
       } catch (Exception ex) {
           Assert.assertEquals(null, ex);
       }
    }
    
    static void setupYarnCluster() throws Exception {
    }
    
    static void launchStorm() throws Exception {
    }

    @Test
    public void performActions() throws Exception { 
        try {
            String cmd = "bin/storm-yarn getStormConfig  src/main/resources/master_defaults.yaml --appId " + appId + " --output target/storm.yaml --rmAddr "+rmAddr;
            LOG.info("execute: "+cmd);           
            Runtime.getRuntime().exec(cmd);

            cmd = "bin/storm-yarn addSupervisors src/main/resources/master_defaults.yaml 2 --appId " + appId + " --output target/storm.yaml --rmAddr "+rmAddr;
            LOG.info("execute: "+cmd);           
            Runtime.getRuntime().exec(cmd);

            cmd = "bin/storm-yarn stopNimbus src/main/resources/master_defaults.yaml 2 --appId " + appId + " --output target/storm.yaml --rmAddr "+rmAddr;
            LOG.info("execute: "+cmd);           
            Runtime.getRuntime().exec(cmd);

            cmd = "bin/storm-yarn startNimbus src/main/resources/master_defaults.yaml 2 --appId " + appId + " --output target/storm.yaml --rmAddr "+rmAddr;
            LOG.info("execute: "+cmd);           
            Runtime.getRuntime().exec(cmd);
        } catch (Exception ex) {
            Assert.assertEquals(null, ex);
        }
    }

    @AfterClass
    public static void tearDown() throws IOException, TException {        
        if (yarnCluster != null) {
            yarnCluster.stop();
            yarnCluster = null;
        }
    }

}

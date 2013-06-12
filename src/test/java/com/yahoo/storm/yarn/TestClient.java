package com.yahoo.storm.yarn;

import java.io.IOException;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.thrift7.TException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.storm.yarn.generated.StormMaster;

public class TestClient {
    static final Logger LOG = LoggerFactory.getLogger(TestClient.class);
    private static MiniYARNCluster yarnCluster = null;
    private static Configuration conf = new YarnConfiguration();
    private static StormOnYarn storm_yarn;
    private static StormMaster.Client master_client;
    
    @BeforeClass
    public static void setup() throws Exception {
        LOG.info("Starting up MiniYARN cluster");
        if (yarnCluster == null) {
            yarnCluster = new MiniYARNCluster(TestClient.class.getName(), 1, 1, 1);
            conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
            yarnCluster.init(conf);
            yarnCluster.start();
        }
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            LOG.info("setup thread sleep interrupted. message=" + e.getMessage());
        }   
        
        LOG.info("Starting up Storm_YARN client");
        String appName = "storm-on-yarn-test";
        String queue = "default";
        int amMB =  4096; //4GB
        Map stormConf = Config.readStormConfig();
        storm_yarn = StormOnYarn.launchApplication(appName, queue, amMB,stormConf);
        Assert.assertTrue(storm_yarn.waitUntilLaunched());
        master_client = storm_yarn.getClient();
    }

    @AfterClass
    public static void tearDown() throws IOException, TException {        
        if (master_client != null) {
            try {
                master_client.stopSupervisors();
                master_client.stopUI();
                master_client.stopNimbus();
            } catch (TException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            master_client = null;
        }

        if (storm_yarn != null) {
            storm_yarn.stop();
            storm_yarn = null;
        }
        
        if (yarnCluster != null) {
            yarnCluster.stop();
            yarnCluster = null;
        }
    }

    @Test
    public void testGetConfig() throws Exception {
        String storm_config_json = master_client.getStormConf();
        LOG.info("Retrieved Storm config:"+storm_config_json);        
        Assert.assertNotNull(storm_config_json);
    }
}

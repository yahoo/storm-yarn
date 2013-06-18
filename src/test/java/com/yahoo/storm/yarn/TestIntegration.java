package com.yahoo.storm.yarn;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.thrift7.TException;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.google.common.base.Joiner;

public class TestIntegration {
    static final Logger LOG = LoggerFactory.getLogger(TestIntegration.class);
    static MiniYARNCluster yarnCluster = null;
    static Configuration conf = new YarnConfiguration();
    static String yarnRmAddr;
    static String schedulerAddr;
    static String appId;
    static EmbeddedZKServer zkServer;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @BeforeClass
    public static void setup() {
        try {
            zkServer = new EmbeddedZKServer();
            zkServer.start();
            
            LOG.info("Starting up MiniYARN cluster");
            if (yarnCluster == null) {
                yarnCluster = new MiniYARNCluster(TestIntegration.class.getName(), 1, 1, 1);
                conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
                yarnCluster.init(conf);
                yarnCluster.start();
            }
            
            sleep(2000);
            yarnRmAddr = yarnCluster.getConfig().get(YarnConfiguration.RM_ADDRESS);
            schedulerAddr = yarnCluster.getConfig().get(YarnConfiguration.RM_SCHEDULER_ADDRESS);

            String storm_home = getStormHomePath();
            if (storm_home == null) {
                throw new RuntimeException("Storm home was not found."
                        + "  Make sure to include storm in the PATH.");
            }
            LOG.info("Will be using storm found on PATH at "+storm_home);
            System.setProperty("storm.home", storm_home);

            //create a storm configuration file with zkport 
            Map storm_conf = Config.readStormConfig();
            Util.rmNulls(storm_conf);
            storm_conf.put(backtype.storm.Config.STORM_ZOOKEEPER_PORT, zkServer.port());
            File configFile = new File("target/storm.yaml");
            Yaml yaml = new Yaml();
            yaml.dump(storm_conf, new FileWriter(configFile));

            List<String> cmd = java.util.Arrays.asList("bin/storm-yarn",
                    "launch",
                    "target/storm.yaml",
                    "--stormZip",
                    "lib/storm.zip",
                    "--appname",
                    "storm-on-yarn-test",
                    "--output",
                    "target/appId.txt",
                    "--rmAddr",
                    yarnRmAddr, 
                    "--schedulerAddr",
                    schedulerAddr);
            LOG.info("Launch ing stor cluster:"+cmd.toString());  
            int status = execute(cmd);
            LOG.info("Launch completed with status code:"+status);           

            //wait for Storm cluster to be fully luanched
            sleep(30000); 

            BufferedReader reader = new BufferedReader(new FileReader ("target/appId.txt"));
            appId = reader.readLine();
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
        Process proc;

        try {
            List<String> cmd = java.util.Arrays.asList("bin/storm-yarn",
                    "getStormConfig",
                    "src/main/resources/master_defaults.yaml",
                    "--appId",
                    appId,
                    "--output",
                    "target/storm1.yaml",
                    "--rmAddr",
                    yarnRmAddr);
            execute(cmd);
            sleep(1000);

            cmd = java.util.Arrays.asList("bin/storm-yarn",
                    "addSupervisors",
                    "src/main/resources/master_defaults.yaml",
                    "2",
                    "--appId",
                    appId,                    
                    "--rmAddr",
                    yarnRmAddr);
            execute(cmd);
            sleep(1000);

            cmd = java.util.Arrays.asList("bin/storm-yarn",
                    "stopNimbus",
                    "src/main/resources/master_defaults.yaml",
                    "--appId",
                    appId,                    
                    "--rmAddr",
                    yarnRmAddr);
            execute(cmd);
            sleep(1000);

            cmd = java.util.Arrays.asList("bin/storm-yarn",
                    "startNimbus",
                    "src/main/resources/master_defaults.yaml",
                    "--appId",
                    appId,                    
                    "--rmAddr",
                    yarnRmAddr);
            execute(cmd);
            sleep(1000);

            cmd = java.util.Arrays.asList("bin/storm-yarn",
                    "stopUI",
                    "src/main/resources/master_defaults.yaml",
                    "--appId",
                    appId,                    
                    "--rmAddr",
                    yarnRmAddr);
            execute(cmd);
            sleep(1000);

            cmd = java.util.Arrays.asList("bin/storm-yarn",
                    "startUI",
                    "src/main/resources/master_defaults.yaml",
                    "--appId",
                    appId,                    
                    "--rmAddr",
                    yarnRmAddr);
            execute(cmd);
            sleep(1000);
        } catch (Exception ex) {
            Assert.assertEquals(null, ex);
        }
    }

    @AfterClass
    public static void tearDown() throws IOException, TException {        
        //shutdown YARN cluster
        if (yarnCluster != null) {
            LOG.info("shutdown MiniYarn cluster");
            yarnCluster.stop();
            yarnCluster = null;
        }
        sleep(1000);
        
        //shutdown Zookeeper server
        if (zkServer != null) {
            zkServer.stop();
            zkServer = null;
        }
   }

    private static int execute(List<String> cmd) throws InterruptedException, IOException {
        LOG.info("execute: "+ Joiner.on(" ").join(cmd));           
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectError(Redirect.INHERIT).redirectOutput(Redirect.INHERIT);
        pb.redirectErrorStream(true);
        pb.redirectOutput();
        Map env = pb.environment();
        env.putAll(System.getenv());
        Process proc = pb.start();
        int status = proc.waitFor();
        return status;
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
}

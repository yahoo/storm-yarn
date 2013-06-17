package com.yahoo.storm.yarn;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

public class EmbeddedZKServer {

    private static final Log LOG = LogFactory.getLog(EmbeddedZKServer.class);
    private NIOServerCnxnFactory zkFactory;
    private int zkport;
    
    void start() throws IOException, InterruptedException {
        LOG.info("Starting up embedded Zookeeper server");
        File localfile = new File("./target/zookeeper.data");
        ZooKeeperServer zkServer;
        zkServer = new ZooKeeperServer(localfile, localfile, 2000);
        NIOServerCnxnFactory zkFactory = new NIOServerCnxnFactory();
        for (zkport = 60000; true; zkport++)
            try {
                zkFactory.configure(new InetSocketAddress(zkport), 10);
                break;
            } catch (BindException e) {
                if (zkport == 65535) throw new IOException("Fail to find a port for Zookeeper server to bind");
            }
        LOG.info("Zookeeper port allocated:"+zkport);
        zkFactory.startup(zkServer);
    }
    
    int port() {
        return zkport;
    }
    
    void stop() {
        LOG.info("shutdown embedded zookeeper server");
        zkFactory.shutdown();
        zkFactory = null;
    }
}

package com.yahoo.storm.yarn;

import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.SupervisorSummary;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.utils.NimbusClient;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.storm.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by ding on 14/11/17.
 */
class StormClusterChecker extends Thread {

    private static Logger LOG = LoggerFactory.getLogger(StormClusterChecker.class);
    private Map<String, Object> stormConf;
    private final StormMasterServerHandler master;
    private Nimbus.Iface nimbus;

    public StormClusterChecker(Map<String, Object> stormConf, StormMasterServerHandler master) {
        this.stormConf = stormConf;
        this.master = master;
        setDaemon(true);
        setName("storm cluster checker thread");
    }

    @Override
    public void run() {
        LOG.info("Try to connect storm nimbus");
        while (true) {
            while (nimbus == null) {
                try {
                    Thread.sleep(10000);
                    nimbus = NimbusClient.getConfiguredClient(stormConf).getClient();
                    LOG.info("Connected to storm nimbus, start checker...");
                } catch (Exception e) {
                }
            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                break;
            }
            try {
                ClusterSummary stormCluster = nimbus.getClusterInfo();
                int totalNumWorkers = stormCluster.get_supervisors().stream()
                        .mapToInt(SupervisorSummary::get_num_workers).sum();
                int totalUsed = stormCluster.get_topologies().stream().mapToInt(TopologySummary::get_num_workers).sum();
                if (totalUsed >= totalNumWorkers) {
                    LOG.info("Need more workers, add 1 supervisor");
                    master.addSupervisors(1);
                }else{
                    int oneSupervisorWorkersNum = stormCluster.get_supervisors().get(0).get_num_workers();
                    int numOfVacant = (totalNumWorkers - totalUsed) ;
                    if(numOfVacant > oneSupervisorWorkersNum){
                        Iterator<Container> it = master.getContainerInfo().iterator();
                        String containerID;
                        if (it.hasNext()) {
                            containerID = it.next().getId().toString();
                            LOG.info("remove a supervisor " + containerID);
                            master.removeSupervisors(containerID);
                        }
                    }
                }
            } catch (TException e) {
                nimbus = null;
            }
        }
    }
}
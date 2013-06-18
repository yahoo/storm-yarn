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
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.security.auth.ThriftServer;
import backtype.storm.utils.Utils;

import com.yahoo.storm.yarn.generated.StormMaster;
import com.yahoo.storm.yarn.generated.StormMaster.Processor;

public class MasterServer extends ThriftServer {
    private static final Logger LOG = LoggerFactory.getLogger(MasterServer.class);
    private static ApplicationAttemptId _appAttemptID;
    private StormMasterServerHandler _handler;

    private Thread initAndStartHeartbeat(final StormAMRMClient client,
            final BlockingQueue<Container> launcherQueue,
            final int heartBeatIntervalMs) {
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    while (client.getServiceState() == Service.STATE.STARTED &&
                            !Thread.currentThread().isInterrupted()) {

                        Thread.sleep(heartBeatIntervalMs); // heart beats once per second

                        // We always send 0% progress.
                        AllocateResponse allocResponse = client.allocate(0.0f);

                        if (allocResponse.getAMResponse().getReboot()) {
                            LOG.info("Got Reboot from the RM");
                            _handler.stop();
                            System.exit(0);
                        }

                        List<Container> allocatedContainers = allocResponse.getAMResponse().getAllocatedContainers();
                        if (allocatedContainers.size() > 0) {
                            // Add newly allocated containers to the client.
                            LOG.debug("HB: Received allocated containers (" + allocatedContainers.size() + ")");
                            client.addAllocatedContainers(allocatedContainers);
                            if (client.supervisorsAreToRun()) {
                                LOG.debug("HB: Supervisors are to run, so queueing (" + allocatedContainers.size() + ") containers...");
                                launcherQueue.addAll(allocatedContainers);
                            } else {
                                LOG.debug("HB: Supervisors are to stop, so releasing all containers...");
                                client.stopAllSupervisors();
                            }
                        }

                        List<ContainerStatus> completedContainers =
                                allocResponse.getAMResponse().getCompletedContainersStatuses();

                        if (completedContainers.size() > 0 && client.supervisorsAreToRun()) {
                            LOG.debug("HB: Containers completed (" + completedContainers.size() + "), so releasing them.");
                            client.startAllSupervisors();
                        }

                    }
                } catch (Throwable t) {
                    // Something happened we could not handle.  Make sure the AM goes
                    // down so that we are not surprised later on that our heart
                    // stopped..
                    LOG.warn("Unexpected exception "+t.toString(), t);
                    _handler.stop();
                    System.exit(1);
                }
            }
        };
        thread.start();
        return thread;
    }

    public static void main(String[] args) throws Exception {
        LOG.info("Starting the AM!!!!");

        Options opts = new Options();
        opts.addOption("app_attempt_id", true, "App Attempt ID. Not to be used " +
                "unless for testing purposes");

        CommandLine cl = new GnuParser().parse(opts, args);

        Map<String, String> envs = System.getenv();
        _appAttemptID = Records.newRecord(ApplicationAttemptId.class);
        if (cl.hasOption("app_attempt_id")) {
            String appIdStr = cl.getOptionValue("app_attempt_id", "");
            _appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
        } else if (envs.containsKey(ApplicationConstants.AM_CONTAINER_ID_ENV)) {
            ContainerId containerId = 
                    ConverterUtils.toContainerId(envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV));
            _appAttemptID = containerId.getApplicationAttemptId();
        } else {
            throw new IllegalArgumentException("Container and application attempt IDs not set in the environment");
        }

        @SuppressWarnings("rawtypes")
        Map storm_conf = Config.readStormConfig(null);
        Util.rmNulls(storm_conf);

        YarnConfiguration hadoopConf = new YarnConfiguration();

        StormAMRMClient rmClient =
                new StormAMRMClient(_appAttemptID, storm_conf, hadoopConf);
        rmClient.init(hadoopConf);
        rmClient.start();

        BlockingQueue<Container> launcherQueue = new LinkedBlockingQueue<Container>();

        MasterServer server = new MasterServer(storm_conf, rmClient);;
        try {
            InetSocketAddress addr =
                    NetUtils.createSocketAddr(hadoopConf.get(YarnConfiguration.RM_SCHEDULER_ADDRESS,
                            YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));
            int port = Utils.getInt(storm_conf.get(Config.MASTER_THRIFT_PORT));
            RegisterApplicationMasterResponse resp =
                    rmClient.registerApplicationMaster(addr.getHostName(), port, null);
            LOG.info("Got a registration response "+resp);
            LOG.info("Max Capability "+resp.getMaximumResourceCapability());
            rmClient.setMaxResource(resp.getMaximumResourceCapability());
            LOG.info("Starting HB thread");
            server.initAndStartHeartbeat(rmClient, launcherQueue,
                    (Integer) storm_conf
                    .get(Config.MASTER_HEARTBEAT_INTERVAL_MILLIS));
            LOG.info("Starting launcher");
            initAndStartLauncher(rmClient, launcherQueue);
            rmClient.startAllSupervisors();
            LOG.info("Starting Master Thrift Server");
            server.serve();
            LOG.info("StormAMRMClient::unregisterApplicationMaster");
            rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
                    "AllDone", null);
        } finally {
            LOG.info("Stop Master Thrift Server");
            server.stop();
            LOG.info("Stop RM client");
            rmClient.stop();
        }
        LOG.debug("See Ya!!!");
        System.exit(0);
    }

    private static void initAndStartLauncher(final StormAMRMClient client,
            final BlockingQueue<Container> launcherQueue) {
        Thread thread = new Thread() {
            Container container;
            @Override
            public void run() {
                while (client.getServiceState() == Service.STATE.STARTED &&
                        !Thread.currentThread().isInterrupted()) {
                    try {
                        container = launcherQueue.take();
                        LOG.info("LAUNCHER: Taking container with id ("+container.getId()+") from the queue.");
                        if (client.supervisorsAreToRun()) {
                            LOG.info("LAUNCHER: Supervisors are to run, so launching container id ("+container.getId()+")");
                            client.launchSupervisorOnContainer(container);
                        } else {
                            // Do nothing
                            LOG.info("LAUNCHER: Supervisors are not to run, so not launching container id ("+container.getId()+")");
                        }
                    } catch (InterruptedException e) {
                        if (client.getServiceState() == Service.STATE.STARTED) {
                            LOG.error("Launcher thread interrupted : ", e);
                            System.exit(1);
                        }
                        return;
                    } catch (IOException e) {
                        LOG.error("Launcher thread I/O exception : ", e);
                        System.exit(1);
                    }
                }
            }
        };
        thread.start();
    }

    public MasterServer(@SuppressWarnings("rawtypes") Map storm_conf, 
            StormAMRMClient client) {
        this(storm_conf, new StormMasterServerHandler(storm_conf, client));
    }

    private MasterServer(@SuppressWarnings("rawtypes") Map storm_conf,
            StormMasterServerHandler handler) {
        super(storm_conf, 
                new Processor<StormMaster.Iface>(handler), 
                Utils.getInt(storm_conf.get(Config.MASTER_THRIFT_PORT)));
        try {
            _handler = handler;
            _handler.startNimbus();
            _handler.startUI();

            int numSupervisors =
                    Utils.getInt(storm_conf.get(Config.MASTER_NUM_SUPERVISORS));
            LOG.info("launch " + numSupervisors + " supervisors");
            _handler.addSupervisors(numSupervisors);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   
    
    public void stop() {
        super.stop();
        if (_handler != null) {
            _handler.stop();
            _handler = null;
        }
    }
}

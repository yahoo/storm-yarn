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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.client.api.impl.NMClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


class StormAMRMClient extends AMRMClientImpl<ContainerRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(StormAMRMClient.class);

    @SuppressWarnings("rawtypes")
    private final Map storm_conf;
    private final YarnConfiguration hadoopConf;
    private final Priority DEFAULT_PRIORITY = Records.newRecord(Priority.class);
    private final Set<Container> containers;
    private volatile boolean supervisorsAreToRun = false;
    private AtomicInteger numSupervisors;
    private Resource maxResourceCapability;
    private ApplicationAttemptId appAttemptId;
    private NMClientImpl nmClient;
    private YarnClient yarnClient;
    private Set<String> requestingNodes = new HashSet<>();

    public StormAMRMClient(ApplicationAttemptId appAttemptId,
                           @SuppressWarnings("rawtypes") Map storm_conf,
                           YarnConfiguration hadoopConf) {
        this.appAttemptId = appAttemptId;
        this.storm_conf = storm_conf;
        this.hadoopConf = hadoopConf;
        Integer pri = Utils.getInt(storm_conf.get(Config.MASTER_CONTAINER_PRIORITY));
        this.DEFAULT_PRIORITY.setPriority(pri);
        this.containers = new TreeSet<Container>();
        numSupervisors = new AtomicInteger(0);

        // start am nm client
        nmClient = (NMClientImpl) NMClient.createNMClient();
        nmClient.init(hadoopConf);
        nmClient.start();
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(hadoopConf);
        yarnClient.start();
    }

    public synchronized void removeCompletedContainers(List<ContainerStatus> completedContainers) {
        LOG.debug("remove completed containers...");
        for (ContainerStatus status : completedContainers) {
            Iterator<Container> it = this.containers.iterator();
            while (it.hasNext()) {
                if (it.next().getId().equals(status.getContainerId())) {
                    it.remove();
                    LOG.info("Remove completed container {}", status.getContainerId());
                    break;
                }
            }
        }
    }

    public synchronized void startAllSupervisors() {
        LOG.debug("Starting all supervisors, requesting containers...");
        this.supervisorsAreToRun = true;
        this.addSupervisorsRequest();
    }

    public synchronized void stopAllSupervisors() {
        LOG.debug("Stopping all supervisors, releasing all containers...");
        this.supervisorsAreToRun = false;
        releaseAllSupervisorsRequest();
    }

    private void addSupervisorsRequest() {
        NodeReport[] availableNodes;
        try {
            Set<String> hosts = containers.stream().map(c -> c.getNodeId().getHost()).collect(Collectors.toSet());
            hosts.addAll(requestingNodes);
            availableNodes = yarnClient.getNodeReports(NodeState.RUNNING).stream()
                    .filter(n -> !hosts.contains(n.getNodeId().getHost())).toArray(NodeReport[]::new);
        } catch (Exception e) {
            LOG.warn("can not get node list", e);
            return;
        }
        int requiredNumSupervisors = numSupervisors.getAndSet(0);
        int num = Math.min(requiredNumSupervisors, availableNodes.length);
        LOG.info("Require {} supervisors, and {} nodes available", requiredNumSupervisors, availableNodes.length);
        for (int i = 0; i < num; i++) {
            String node = availableNodes[i].getNodeId().getHost();
            requestingNodes.add(node);
            ContainerRequest req = new ContainerRequest(this.maxResourceCapability,
                    new String[]{node}, // String[] nodes,
                    null, // String[] racks,
                    DEFAULT_PRIORITY, false);
            super.addContainerRequest(req);
        }
    }

    public synchronized boolean addAllocatedContainers(List<Container> containers) {
        for (int i = 0; i < containers.size(); i++) {
            Container c = containers.get(i);
            ContainerRequest req = new ContainerRequest(this.maxResourceCapability,
                    new String[]{c.getNodeId().getHost()}, // String[] nodes,
                    null, // String[] racks,
                    DEFAULT_PRIORITY, false);
            super.removeContainerRequest(req);
            requestingNodes.remove(c.getNodeId().getHost());
        }
        return this.containers.addAll(containers);
    }

    private synchronized void releaseAllSupervisorsRequest() {
        Iterator<Container> it = this.containers.iterator();
        ContainerId id;
        while (it.hasNext()) {
            id = it.next().getId();
            LOG.debug("Releasing container (id:" + id + ")");
            releaseAssignedContainer(id);
            it.remove();
        }
    }

    public synchronized boolean supervisorsAreToRun() {
        return this.supervisorsAreToRun;
    }

    public synchronized void addSupervisors(int number) {
        int num = numSupervisors.addAndGet(number);
        if (this.supervisorsAreToRun) {
            LOG.info("Added " + num + " supervisors, and requesting containers...");
            addSupervisorsRequest();
        } else {
            LOG.info("Added " + num + " supervisors, but not requesting containers now.");
        }
    }

    public void launchSupervisorOnContainer(Container container) throws IOException {
//        containers.
        // create a container launch context
        ContainerLaunchContext launchContext = Records.newRecord(ContainerLaunchContext.class);
        UserGroupInformation user = UserGroupInformation.getCurrentUser();
        try {
            Credentials credentials = user.getCredentials();
            DataOutputBuffer dob = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
            launchContext.setTokens(securityTokens);
        } catch (IOException e) {
            LOG.warn("Getting current user info failed when trying to launch the container"
                    + e.getMessage());
        }

        // CLC: env
        Map<String, String> env = new HashMap<String, String>();
        env.put("STORM_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);

        String java_home = (String) storm_conf.get("storm.yarn.java_home");
        if (java_home == null)
            java_home = System.getenv("JAVA_HOME");
        if (java_home != null && !java_home.isEmpty())
            env.put("JAVA_HOME", java_home);

        launchContext.setEnvironment(env);

        // CLC: local resources includes storm, conf
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        String storm_zip_path = (String) storm_conf.get("storm.zip.path");
        Path zip = new Path(storm_zip_path);
        FileSystem fs = FileSystem.get(hadoopConf);
        String vis = (String) storm_conf.get("storm.zip.visibility");
        if (vis.equals("PUBLIC"))
            localResources.put("storm", Util.newYarnAppResource(fs, zip,
                    LocalResourceType.ARCHIVE, LocalResourceVisibility.PUBLIC));
        else if (vis.equals("PRIVATE"))
            localResources.put("storm", Util.newYarnAppResource(fs, zip,
                    LocalResourceType.ARCHIVE, LocalResourceVisibility.PRIVATE));
        else if (vis.equals("APPLICATION"))
            localResources.put("storm", Util.newYarnAppResource(fs, zip,
                    LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION));

        String userShortName = user.getShortUserName();
        String workingDirectory = hadoopConf.get("yarn.nodemanager.local-dirs") + "/usercache/"+ userShortName
                + "/appcache/" + appAttemptId.getApplicationId() + "/" + container.getId();
        Version stormVersion = Util.getStormVersion();
        String stormHomeInZip = Util.getStormHomeInZip(fs, zip, stormVersion.version());
        String appHome = Util.getApplicationHomeForId(appAttemptId.toString());
        String containerHome = appHome + Path.SEPARATOR + container.getId().getContainerId();
        Map supervisorConf = new HashMap(this.storm_conf);
        Path confDst = Util.createConfigurationFileInFs(fs, containerHome, supervisorConf, this.hadoopConf);
        localResources.put("conf", Util.newYarnAppResource(fs, confDst));

        launchContext.setLocalResources(localResources);

        // CLC: command
        List<String> supervisorArgs = Util.buildSupervisorCommands(this.storm_conf,workingDirectory,stormHomeInZip);
        launchContext.setCommands(supervisorArgs);
        try {
            LOG.info("Use NMClient to launch supervisors in container. ");
            nmClient.startContainer(container, launchContext);
            if (userShortName != null)
                LOG.info("Supervisor log: http://" + container.getNodeHttpAddress() + "/node/containerlogs/"
                        + container.getId().toString() + "/" + userShortName + "/supervisor.log");

        } catch (Exception e) {
            LOG.error("Caught an exception while trying to start a container", e);
            System.exit(-1);
        }
    }

    public void setMaxResource(Resource maximumResourceCapability) {
        this.maxResourceCapability = maximumResourceCapability;
        LOG.info("Max Capability is now " + this.maxResourceCapability);
    }

    public synchronized void removeSupervisors(String hostname) {
        LOG.info("removing container id: " + hostname + this.supervisorsAreToRun);
        if (this.supervisorsAreToRun) {
            LOG.debug("remove the needless supervisors, stop the container...");
            releaseSupervisorsRequest(hostname);
        }else{
            LOG.error("No supervisor is running!!!");
        }
    }

    private synchronized void releaseSupervisorsRequest(String hostname) {
        Iterator<Container> it = this.containers.iterator();
        while (it.hasNext()) {
            ContainerId id;
            id = it.next().getId();
            if(id.toString().equals(hostname)) {
                LOG.debug("Releasing container (id:" + id + ")");
                releaseAssignedContainer(id);
                it.remove();
                break;
            }
        }
    }

    public Set<Container> getAllContainerInfo() {
        return this.containers;
    }
}

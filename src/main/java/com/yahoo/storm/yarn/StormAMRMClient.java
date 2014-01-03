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

import backtype.storm.utils.Utils;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.client.api.impl.NMClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

class StormAMRMClient extends AMRMClientImpl<ContainerRequest>  {
  private static final Logger LOG = LoggerFactory.getLogger(StormAMRMClient.class);

  @SuppressWarnings("rawtypes")
  private final Map storm_conf;
  private final YarnConfiguration hadoopConf;
  private final Priority DEFAULT_PRIORITY = Records.newRecord(Priority.class);
  private final BiMap<NodeId, ContainerId> runningSupervisors;
  private volatile boolean supervisorsAreToRun = false;
  private AtomicInteger numSupervisors;
  private Resource maxResourceCapability;
  private ApplicationAttemptId appAttemptId;
  private NMClientImpl nmClient;

  public StormAMRMClient(ApplicationAttemptId appAttemptId,
                         @SuppressWarnings("rawtypes") Map storm_conf,
                         YarnConfiguration hadoopConf) {
    this.appAttemptId = appAttemptId;
    this.storm_conf = storm_conf;
    this.hadoopConf = hadoopConf;
    Integer pri = Utils.getInt(storm_conf.get(Config.MASTER_CONTAINER_PRIORITY));
    this.DEFAULT_PRIORITY.setPriority(pri);
    numSupervisors = new AtomicInteger(0);
    runningSupervisors = Maps.synchronizedBiMap(HashBiMap.<NodeId,
        ContainerId>create());

    // start am nm client
    nmClient = (NMClientImpl) NMClient.createNMClient();
    nmClient.init(hadoopConf);
    nmClient.start();
  }

  public synchronized void startAllSupervisors() {
    LOG.debug("Starting all supervisors, requesting containers...");
    this.supervisorsAreToRun = true;
    this.addSupervisorsRequest();
  }

  /**
   * Stopping a supervisor by {@link NodeId}
   * @param nodeIds
   */
  public synchronized void stopSupervisors(NodeId... nodeIds) {
    if(LOG.isDebugEnabled()){
      LOG.debug(
          "Stopping supervisors at nodes[" + Arrays.toString(nodeIds) + "], " +
              "releasing all containers.");
    }
    releaseSupervisors(nodeIds);
  }

  /**
   * Need to be able to stop a supervisor by {@link ContainerId}
   * @param containerIds supervisor containers to stop
   */
  public synchronized void stopSupervisors(ContainerId... containerIds) {
    if(LOG.isDebugEnabled()){
      LOG.debug("Stopping supervisors in containers[" +
          Arrays.toString(containerIds) + "], " +
          "releasing all containers.");
    }
    releaseSupervisors(containerIds);
  }

  public synchronized void stopAllSupervisors() {
    LOG.debug("Stopping all supervisors, releasing all containers...");
    this.supervisorsAreToRun = false;
    releaseAllSupervisorsRequest();
  }

  private void addSupervisorsRequest() {
    int num = numSupervisors.getAndSet(0);
    for (int i=0; i<num; i++) {
      ContainerRequest req = new ContainerRequest(this.maxResourceCapability,
              null, // String[] nodes,
              null, // String[] racks,
              DEFAULT_PRIORITY);
      super.addContainerRequest(req);
    }
  }

  /**
   * Add supervisor from allocation. If supervisor is already running there,
   * release the assigned container.
   *
   * @param container Container to make supervisor
   * @return true if supervisor assigned, false if supervisor is not assigned
   */
  public synchronized boolean addAllocatedContainer(Container container) {
      ContainerId containerId = container.getId();
      NodeId nodeId = container.getNodeId();

      //check if supervisor is already running at this host, if so,do not request
      if (!runningSupervisors.containsKey(nodeId)) {
        //add a running supervisor
        addRunningSupervisor(nodeId, containerId);

        ContainerRequest req =
            new ContainerRequest(this.maxResourceCapability, null,
                // String[] nodes,
                null, // String[] racks,
                DEFAULT_PRIORITY);
        super.removeContainerRequest(req);
        return true;
      } else {
        //deallocate this request
        LOG.info("Supervisor already running on node["+nodeId+"]");
        super.releaseAssignedContainer(containerId);
        //since no allocation, increase the number of supervisors to allocate
        //in hopes that another node may be added in the future
        numSupervisors.incrementAndGet();
        return false;
      }
  }

  /**
   * Will add this node and container to the running supervisors. And also
   * add the node to the blacklist.
   * @param nodeId
   * @param containerId
   */
  private void addRunningSupervisor(NodeId nodeId, ContainerId containerId) {
    runningSupervisors.put(nodeId, containerId);
    //add to blacklist, so no more resources are allocated here
    super.updateBlacklist(Lists.newArrayList(nodeId.getHost()), null);
  }

  /**
   * Will remove this node from the running supervisors. And also remove from
   * the blacklist
   * @param nodeId Node to remove
   * @return ContainerId if in the list of running supervisors, null if not
   */
  private ContainerId removeRunningSupervisor(NodeId nodeId) {
    ContainerId containerId = runningSupervisors.remove(nodeId);
    if(containerId != null) {
      super.updateBlacklist(null, Lists.newArrayList(nodeId.getHost()));
    }
    return containerId;
  }

  private synchronized void releaseAllSupervisorsRequest() {
    Set<NodeId> nodeIds = runningSupervisors.keySet();
    this.releaseSupervisors(nodeIds.toArray(new NodeId[nodeIds.size()]));
  }

  /**
   * This is the main entry point to release a supervisor.
   * @param nodeIds
   */
  private synchronized void releaseSupervisors(NodeId... nodeIds) {
    for(NodeId nodeId : nodeIds) {
      //remove from running supervisors list
      ContainerId containerId = removeRunningSupervisor(nodeId);
      if(containerId != null) {
        LOG.debug("Releasing container (id:"+containerId+")");
        //release the containers on the specified nodes
        super.releaseAssignedContainer(containerId);
        //increase the number of supervisors to request on the next heartbeat
        numSupervisors.incrementAndGet();
      }
    }
  }

  private synchronized void releaseSupervisors(ContainerId... containerIds) {
    BiMap<ContainerId, NodeId> inverse = runningSupervisors.inverse();
    for(ContainerId containerId : containerIds) {
      NodeId nodeId = inverse.get(containerId);
      if(nodeId != null) {
        this.releaseSupervisors(nodeId);
      }
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

  public void launchSupervisorOnContainer(Container container)
      throws IOException {
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

    String appHome = Util.getApplicationHomeForId(appAttemptId.toString());
    Path confDst = Util.createConfigurationFileInFs(fs, appHome,
            this.storm_conf, this.hadoopConf);
    localResources.put("conf", Util.newYarnAppResource(fs, confDst));

    launchContext.setLocalResources(localResources);

    // CLC: command
    List<String> supervisorArgs = Util.buildSupervisorCommands(this.storm_conf);
    launchContext.setCommands(supervisorArgs);

    try {
      LOG.info("Use NMClient to launch supervisors in container. ");
      nmClient.startContainer(container, launchContext);

      String userShortName = user.getShortUserName();
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
}

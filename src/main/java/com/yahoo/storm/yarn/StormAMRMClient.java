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
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.net.InetSocketAddress;

import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.AMRMClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ProtoUtils;
import org.apache.hadoop.yarn.util.Records;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.storm.yarn.Config;
import backtype.storm.utils.Utils;

class StormAMRMClient extends AMRMClientImpl {
  private static final Logger LOG = LoggerFactory.getLogger(StormAMRMClient.class);

  @SuppressWarnings("rawtypes")
  private final Map storm_conf;
  private final YarnConfiguration hadoopConf;
  private YarnRPC rpc;
  private final Priority DEFAULT_PRIORITY = Records.newRecord(Priority.class);
  private final Set<Container> containers;
  private volatile boolean supervisorsAreToRun = false;
  private int numSupervisors;
  private Resource maxResourceCapability;

  public StormAMRMClient(ApplicationAttemptId appAttemptId,
      @SuppressWarnings("rawtypes") Map storm_conf, YarnConfiguration hadoopConf) {
    super(appAttemptId);
    this.storm_conf = storm_conf;
    this.hadoopConf = hadoopConf;
    this.rpc = YarnRPC.create(hadoopConf);
    Integer pri = Utils.getInt(storm_conf.get(Config.MASTER_CONTAINER_PRIORITY));
    this.DEFAULT_PRIORITY.setPriority(pri);
    this.containers = new TreeSet<Container>();
  }

  private ContainerRequest setupContainerAskForRM(int numContainers) {
    LOG.info("Creating new ContainerRequest with " + this.maxResourceCapability + " and " + numContainers);
    ContainerRequest request =
        new ContainerRequest(this.maxResourceCapability, null, null,
            DEFAULT_PRIORITY, numContainers);
    LOG.info("Requested container ask: " + request.toString());
    return request;
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
    ContainerRequest req = setupContainerAskForRM(this.numSupervisors);
    super.addContainerRequest(req);
  }
  
  public synchronized boolean
      addAllocatedContainers(List<Container> containers) {
    return this.containers.addAll(containers);
  }

  private synchronized void releaseAllSupervisorsRequest() {
    Iterator<Container> it = this.containers.iterator();
    ContainerId id;
    while (it.hasNext()) {
      id = it.next().getId();
      LOG.debug("Releasing container (id:"+id+")");
      releaseAssignedContainer(id);
      it.remove();
    }
  }
  
  public synchronized boolean supervisorsAreToRun() {
    return this.supervisorsAreToRun;
  }

  public synchronized void addSupervisors(int number) {
    this.numSupervisors += number;
    if (this.supervisorsAreToRun) {
      LOG.info("Added " + number + " supervisors, and requesting containers...");
      addSupervisorsRequest();
    } else {
      LOG.info("Added " + number + " supervisors, but not requesting containers now.");
    }
  }

  protected ContainerManager getCMProxy(Container container, ContainerId containerID) throws IOException {
    NodeId nodeId = container.getNodeId();
    String cmIpPortStr = nodeId.getHost() + ":" + nodeId.getPort();
    final InetSocketAddress cmAddr = NetUtils.createSocketAddr(cmIpPortStr);

    UserGroupInformation user;
    if (UserGroupInformation.isSecurityEnabled()) {
      LOG.info("Connecting to ContainerManager via UGI with security mode");
      // the user in createRemoteUser in this context has to be ContainerID
      user = UserGroupInformation.createRemoteUser(containerID.toString());
      user.addToken(ProtoUtils.convertFromProtoFormat(container.getContainerToken(), cmAddr));
    } else {
      LOG.info("Connecting to ContainerManager ...");
      user = UserGroupInformation.getCurrentUser();
    }

    ContainerManager proxy = user.doAs(new PrivilegedAction<ContainerManager>() {
      @Override
      public ContainerManager run() {
        return (ContainerManager) rpc.getProxy(ContainerManager.class,
                cmAddr, hadoopConf);
      }
    });
    return proxy;
  }

  public void launchSupervisorOnContainer(Container container)
      throws IOException {
    ContainerId containerID = container.getId();
    LOG.info("launchSupervisorOnContainer() for containerid=" + containerID);
    ContainerManager proxy = getCMProxy(container, containerID);

    ContainerLaunchContext launchContext = Records.newRecord(ContainerLaunchContext.class);

    launchContext.setContainerId(containerID);
    launchContext.setResource(container.getResource());

    String userShortName = null;
    try {
      UserGroupInformation user = UserGroupInformation.getCurrentUser();
      userShortName = user.getShortUserName();
      launchContext.setUser(userShortName);

      Credentials credentials = user.getCredentials();
      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      launchContext.setContainerTokens(securityTokens);
    } catch (IOException e) {
      LOG.warn("Getting current user info failed when trying to launch the container"
              + e.getMessage());
    }

    Map<String, String> env = new HashMap<String, String>();
    env.put("STORM_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    launchContext.setEnvironment(env);


    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();
    String stormVersion = Util.getStormVersion(this.storm_conf);
    String storm_zip_path = (String)storm_conf.get("storm.zip.path");
    Path zip = new Path(storm_zip_path);
    FileSystem fs = FileSystem.get(this.hadoopConf);
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

    String appHome = Util.getApplicationHomeForId(this.appAttemptId.toString());
    
    Path dirDst = Util.createConfigurationFileInFs(
            fs, appHome, this.storm_conf, this.hadoopConf);
    
    localResources.put("conf", Util.newYarnAppResource(fs, dirDst));
    
    launchContext.setLocalResources(localResources);
    
    List<String> supervisorArgs = Util.buildSupervisorCommands(this.storm_conf);
    
    launchContext.setCommands(supervisorArgs);

   
    StartContainerRequest startRequest =
        Records.newRecord(StartContainerRequest.class);
    startRequest.setContainerLaunchContext(launchContext);
    
    LOG.info("launchSupervisorOnContainer: startRequest prepared, calling startContainer. "+startRequest);
    try {
      StartContainerResponse response = proxy.startContainer(startRequest);
      if (userShortName != null)
        LOG.info("Supervisor log: http://" + container.getNodeHttpAddress() + "/node/containerlogs/"
                + containerID.toString() + "/" + userShortName + "/supervisor.log");
    } catch (Exception e) {
       LOG.error("Caught an exception while trying to start a container", e);
       System.exit(-1);
    }
  }

  public void setMaxResource(Resource maximumResourceCapability) {
    this.maxResourceCapability = maximumResourceCapability;
    LOG.info("Max Capability is now "+this.maxResourceCapability);
  }
}

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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.net.InetSocketAddress;

import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.AMRMClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    LOG.info("Creating new ContainerRequest with "+this.maxResourceCapability+" and "+numContainers);
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

  public void launchSupervisorOnContainer(Container container)
      throws IOException {
    LOG.info("Connecting to ContainerManager for containerid=" + container.getId());
    String cmIpPortStr = container.getNodeId().getHost() + ":"
          + container.getNodeId().getPort();
    InetSocketAddress cmAddress = NetUtils.createSocketAddr(cmIpPortStr);
    LOG.info("Connecting to ContainerManager at " + cmIpPortStr);
    ContainerManager proxy = ((ContainerManager) rpc.getProxy(ContainerManager.class, cmAddress, hadoopConf));


    LOG.info("launchSupervisorOnContainer( id:"+container.getId()+" )");
    ContainerLaunchContext launchContext = Records
        .newRecord(ContainerLaunchContext.class);

    launchContext.setContainerId(container.getId()); 
    launchContext.setResource(container.getResource());

    try {
      launchContext.setUser(UserGroupInformation.getCurrentUser().getShortUserName());
    } catch (IOException e) {
      LOG.info("Getting current user info failed when trying to launch the container"
          + e.getMessage());
    }
 
    Map<String, String> env = new HashMap<String, String>();
    launchContext.setEnvironment(env);


    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();
    String stormVersion = Util.getStormVersion(this.storm_conf);
    Path zip = new Path("/lib/storm/"+stormVersion+"/storm.zip");
    FileSystem fs = FileSystem.get(this.hadoopConf);
    localResources.put("storm", Util.newYarnAppResource(fs, zip,
        LocalResourceType.ARCHIVE, LocalResourceVisibility.PUBLIC));
    
    String appHome = ".storm/" + this.appAttemptId;
    
    Path dirDst =
        Util.createConfigurationFileInFs(fs, appHome, this.storm_conf);
    
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
      LOG.info("Got a start container response "+response);
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

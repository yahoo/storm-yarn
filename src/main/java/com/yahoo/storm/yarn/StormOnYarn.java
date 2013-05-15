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
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.thrift7.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.storm.yarn.generated.StormMaster;

public class StormOnYarn {
  private static final Logger LOG = LoggerFactory.getLogger(StormOnYarn.class);
  
  private YarnClient _yarn;
  private YarnConfiguration _hadoopConf;
  private ApplicationId _appId;
  @SuppressWarnings("rawtypes")
  private Map _stormConf;
  private MasterClient _client = null;
  
  private StormOnYarn(@SuppressWarnings("rawtypes") Map stormConf) {
    this(null, stormConf);
  }
  
  private StormOnYarn(ApplicationId appId,
      @SuppressWarnings("rawtypes") Map stormConf) {
    _stormConf = stormConf;
    _appId = appId;
    _hadoopConf = new YarnConfiguration();
    _yarn = new YarnClientImpl();
    _yarn.init(_hadoopConf);
    _yarn.start();
  }
  
  public void stop() {
    if(_client != null) {
      _client.close();
    }
    _yarn.stop();
  }
  
  public ApplicationId getAppId() {
    //TODO make this immutable
    return _appId;
  }
  
  @SuppressWarnings("unchecked")
  private synchronized StormMaster.Client getClient() throws YarnRemoteException {
    if (_client == null) {
      //TODO need a way to force this to reconnect in case of an error
      ApplicationReport report = _yarn.getApplicationReport(_appId);
      String host = report.getHost();
      _stormConf.put(Config.MASTER_HOST, host);
      int port = report.getRpcPort();
      _stormConf.put(Config.MASTER_THRIFT_PORT, port);
      LOG.info("Attaching to "+host+":"+port+" to talk to app master");
      //TODO need a better work around to the config not being set.
      _stormConf.put(Config.MASTER_TIMEOUT_SECS, 10);
      _client = MasterClient.getConfiguredClient(_stormConf);
    }
    return _client.getClient();
  }
  
  public void stopNimbus() throws YarnRemoteException, TException {
    getClient().send_stopNimbus();
  }
 
  private void launchApp(String appName, String queue, int amMB) throws Exception {
    GetNewApplicationResponse app = _yarn.getNewApplication();
    _appId = app.getApplicationId();
    
    if(amMB > app.getMaximumResourceCapability().getMemory()) {
      //TODO need some sanity checks
      amMB = app.getMaximumResourceCapability().getMemory();
    }
    ApplicationSubmissionContext appContext = 
      Records.newRecord(ApplicationSubmissionContext.class);
    appContext.setApplicationId(app.getApplicationId());
    appContext.setApplicationName(appName);
    appContext.setQueue(queue);
    
    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = Records
        .newRecord(ContainerLaunchContext.class);
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

    // set local resources for the application master
    // local files or archives as needed
    // In this scenario, the jar file for the application master is part of the
    // local resources
    LOG.info("Copy App Master jar from local filesystem and add to local environment");
    // Copy the application master jar to the filesystem
    // Create a local resource to point to the destination jar path
    String appMasterJar = findContainingJar(MasterServer.class);
    FileSystem fs = FileSystem.get(_hadoopConf);
    Path src = new Path(appMasterJar);
    String appHome =  ".storm/" + _appId;
    Path dst = new Path(fs.getHomeDirectory(), appHome + "/AppMaster.jar");
    fs.copyFromLocalFile(false, true, src, dst);
    localResources.put("AppMaster.jar", Util.newYarnAppResource(fs, dst));

    // 
    String stormVersion = Util.getStormVersion(_stormConf);
    Path zip = new Path("/lib/storm/"+stormVersion+"/storm.zip");
    localResources.put("storm", Util.newYarnAppResource(fs, zip,
        LocalResourceType.ARCHIVE, LocalResourceVisibility.PUBLIC));

    Path dirDst = Util.createConfigurationFileInFs(fs, appHome, _stormConf);
    // establish a symbolic link to conf directory
    //
    localResources.put("conf", Util.newYarnAppResource(fs, dirDst));

    // Set local resource info into app master container launch context
    amContainer.setLocalResources(localResources);

    // Set the env variables to be setup in the env where the application master
    // will be run
    LOG.info("Set the environment for the application master");
    Map<String, String> env = new HashMap<String, String>();
    // add the runtime classpath needed for tests to work
    Apps.addToEnvironment(env, Environment.CLASSPATH.name(), "./conf");
    Apps.addToEnvironment(env, Environment.CLASSPATH.name(), "./AppMaster.jar");
    //TODO need a better way to get the storm .zip created and put where it needs to go.
    Apps.addToEnvironment(env, Environment.CLASSPATH.name(), "./storm/storm/*");
    Apps.addToEnvironment(env, Environment.CLASSPATH.name(), "./storm/storm/lib/*");
    for (String c : _hadoopConf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      Apps.addToEnvironment(env, Environment.CLASSPATH.name(), c
          .trim());
    }
    
    env.put("appJar", appMasterJar);
    env.put("appName", appName);
    env.put("appId", new Integer(_appId.getId()).toString());
    amContainer.setEnvironment(env);

    // Set the necessary command to execute the application master
    Vector<String> vargs = new Vector<String>();

    // Set java executable command
    LOG.info("Setting up app master command");
    // TODO need a better way to do debugging
    vargs.add("find");
    vargs.add(".");
    vargs.add("-follow");
    vargs.add("|");
    vargs.add("xargs");
    vargs.add("ls");
    vargs.add("-ld");
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/files");
    vargs.add("&&");
    vargs.add("echo");
    vargs.add("$CLASSPATH");
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/classpath");
    vargs.add("&&");
    vargs.add("echo");
    vargs.add("$PWD");
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/pwd");
    vargs.add("&&");
    vargs.add("java");
    vargs.add("-Dstorm.home=./storm/storm/");
    //vargs.add("-verbose:class");
    vargs.add("com.yahoo.storm.yarn.MasterServer");
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

    amContainer.setCommands(vargs);

    // Set up resource type requirements
    // For now, only memory is supported so we set memory requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(amMB);
    amContainer.setResource(capability);
    appContext.setAMContainerSpec(amContainer);
    
    _yarn.submitApplication(appContext);
  }
  
  /** 
   * Find a jar that contains a class of the same name, if any.
   * It will return a jar file, even if that is not the first thing
   * on the class path that has a class with the same name.
   * 
   * @param my_class the class to find.
   * @return a jar file that contains the class, or null.
   * @throws IOException on any error
   */
  public static String findContainingJar(Class<MasterServer> my_class) throws IOException {
    ClassLoader loader = my_class.getClassLoader();
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
    for(Enumeration<URL> itr = loader.getResources(class_file);
        itr.hasMoreElements();) {
      URL url = itr.nextElement();
      if ("jar".equals(url.getProtocol())) {
        String toReturn = url.getPath();
        if (toReturn.startsWith("file:")) {
          toReturn = toReturn.substring("file:".length());
        }
        // URLDecoder is a misnamed class, since it actually decodes
        // x-www-form-urlencoded MIME type rather than actual
        // URL encoding (which the file path has). Therefore it would
        // decode +s to ' 's which is incorrect (spaces are actually
        // either unencoded or encoded as "%20"). Replace +s first, so
        // that they are kept sacred during the decoding process.
        toReturn = toReturn.replaceAll("\\+", "%2B");
        toReturn = URLDecoder.decode(toReturn, "UTF-8");
        return toReturn.replaceAll("!.*$", "");
      }
    }
    return null;
  }
  
  public static StormOnYarn launchApplication(String appName, String queue, 
      int amMB, @SuppressWarnings("rawtypes") Map stormConf) throws Exception {
    StormOnYarn storm = new StormOnYarn(stormConf);
    storm.launchApp(appName, queue, amMB);
    return storm;
  }
  
  public static StormOnYarn attachToApp(String appId,
      @SuppressWarnings("rawtypes") Map stormConf) {
    return new StormOnYarn(ConverterUtils.toApplicationId(appId), stormConf);
  }
}

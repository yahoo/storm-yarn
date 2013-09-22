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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
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
        _hadoopConf = new YarnConfiguration();
        _yarn = YarnClient.createYarnClient();
        _yarn.init(_hadoopConf);
        _stormConf = stormConf;
        _appId = appId;
        _yarn.init(_hadoopConf);
        _yarn.start();
    }

    public void stop() {
        if (_client != null) {
            _client.close();
        }
        _yarn.stop();
    }

    public ApplicationId getAppId() {
        // TODO make this immutable
        return _appId;
    }

    @SuppressWarnings("unchecked")
    public synchronized StormMaster.Client getClient() throws YarnException, IOException {
        if (_client == null) {
            // TODO need a way to force this to reconnect in case of an error
            ApplicationReport report = _yarn.getApplicationReport(_appId);
            LOG.info("application report for " + _appId + " :"
                    + report.getHost() + ":" + report.getRpcPort());
            String host = report.getHost();
            if (host == null) {
                throw new RuntimeException(
                        "No host returned for Application Master " + _appId);
            }
            if (_stormConf == null) {
                _stormConf = new HashMap<Object, Object>();
            }
            _stormConf.put(Config.MASTER_HOST, host);
            int port = report.getRpcPort();
            _stormConf.put(Config.MASTER_THRIFT_PORT, port);
            LOG.info("Attaching to " + host + ":" + port
                    + " to talk to app master " + _appId);
            // TODO need a better work around to the config not being set.
            _stormConf.put(Config.MASTER_TIMEOUT_SECS, 10);
            _client = MasterClient.getConfiguredClient(_stormConf);
        }
        return _client.getClient();
    }

    private void launchApp(String appName, String queue, int amMB,
            String storm_zip_location) throws Exception {
        LOG.debug("StormOnYarn:launchApp() ...");
        YarnClientApplication client_app = _yarn.createApplication();
        GetNewApplicationResponse app = client_app.getNewApplicationResponse();
        _appId = app.getApplicationId();
        LOG.debug("_appId:" + _appId);

        if (amMB > app.getMaximumResourceCapability().getMemory()) {
            // TODO need some sanity checks
            amMB = app.getMaximumResourceCapability().getMemory();
        }
        ApplicationSubmissionContext appContext = Records
                .newRecord(ApplicationSubmissionContext.class);
        appContext.setApplicationId(app.getApplicationId());
        appContext.setApplicationName(appName);
        appContext.setQueue(queue);

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = Records
                .newRecord(ContainerLaunchContext.class);
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        // set local resources for the application master
        // local files or archives as needed
        // In this scenario, the jar file for the application master is part of
        // the local resources
        LOG.info("Copy App Master jar from local filesystem and add to local environment");
        // Copy the application master jar to the filesystem
        // Create a local resource to point to the destination jar path
        String appMasterJar = findContainingJar(MasterServer.class);
        FileSystem fs = FileSystem.get(_hadoopConf);
        Path src = new Path(appMasterJar);
        String appHome = Util.getApplicationHomeForId(_appId.toString());
        Path dst = new Path(fs.getHomeDirectory(), appHome + Path.SEPARATOR
                + "AppMaster.jar");
        fs.copyFromLocalFile(false, true, src, dst);
        localResources.put("AppMaster.jar", Util.newYarnAppResource(fs, dst));

        String stormVersion = Util.getStormVersion(_stormConf);
        Path zip;
        if (storm_zip_location != null) {
            zip = new Path(storm_zip_location);
        } else {
            zip = new Path("/lib/storm/" + stormVersion + "/storm.zip");
        }
        _stormConf.put("storm.zip.path", zip.makeQualified(fs).toUri().getPath());
        LocalResourceVisibility visibility = LocalResourceVisibility.PUBLIC;
        _stormConf.put("storm.zip.visibility", "PUBLIC");
        if (!Util.isPublic(fs, zip)) {
          visibility = LocalResourceVisibility.APPLICATION;
          _stormConf.put("storm.zip.visibility", "APPLICATION");
        }
        localResources.put("storm", Util.newYarnAppResource(fs, zip, LocalResourceType.ARCHIVE, visibility));

        Path confDst = Util.createConfigurationFileInFs(fs, appHome, _stormConf,
                _hadoopConf);
        // establish a symbolic link to conf directory
        localResources.put("conf", Util.newYarnAppResource(fs, confDst));

        // Setup security tokens
        Path[] paths = new Path[3];
        paths[0] = dst;
        paths[1] = zip;
        paths[2] = confDst;
        Credentials credentials = new Credentials();
        TokenCache.obtainTokensForNamenodes(credentials, paths, _hadoopConf);
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

        //security tokens for HDFS distributed cache
        amContainer.setTokens(securityTokens);

        // Set local resource info into app master container launch context
        amContainer.setLocalResources(localResources);

        // Set the env variables to be setup in the env where the application
        // master will be run
        LOG.info("Set the environment for the application master");
        Map<String, String> env = new HashMap<String, String>();
        // add the runtime classpath needed for tests to work
        Apps.addToEnvironment(env, Environment.CLASSPATH.name(), "./conf");
        Apps.addToEnvironment(env, Environment.CLASSPATH.name(),
                "./AppMaster.jar");

        //Make sure that AppMaster has access to all YARN JARs
        List<String> yarn_classpath_cmd = java.util.Arrays.asList("yarn", "classpath");
        ProcessBuilder pb = new ProcessBuilder(yarn_classpath_cmd).redirectError(Redirect.INHERIT);
        File classpath_file = File.createTempFile("storm_yarn_classpath", "txt");
        pb.redirectOutput(ProcessBuilder.Redirect.to(classpath_file));
        pb.environment().putAll(System.getenv());
        Process proc = pb.start();
        proc.waitFor();
        BufferedReader reader = new BufferedReader(new FileReader(classpath_file));
        StringBuilder yarn_class_path = new StringBuilder();
        String line = "";
        while( ( line = reader.readLine() ) != null)
            yarn_class_path.append(line);
        reader.close();
        Apps.addToEnvironment(env, Environment.CLASSPATH.name(), yarn_class_path.toString());

        //storm classpath
        String stormHomeInZip = Util.getStormHomeInZip(fs, zip, stormVersion);
        Apps.addToEnvironment(env, Environment.CLASSPATH.name(), 
                "./storm/" + stormHomeInZip + "/*");
        Apps.addToEnvironment(env, Environment.CLASSPATH.name(), 
                "./storm/" + stormHomeInZip + "/lib/*");

        String java_home = System.getenv("JAVA_HOME");
        if (java_home!=null && !java_home.isEmpty())
          env.put("JAVA_HOME", java_home);
        env.put("appJar", appMasterJar);
        env.put("appName", appName);
        env.put("appId", new Integer(_appId.getId()).toString());
        env.put("STORM_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);
        amContainer.setEnvironment(env);

        // Set the necessary command to execute the application master
        Vector<String> vargs = new Vector<String>();

        // TODO need a better way to do debugging
        if (java_home != null && !java_home.isEmpty())
          vargs.add(System.getenv("JAVA_HOME") + "/bin/java");
        else
          vargs.add("java");
        vargs.add("-Dstorm.home=./storm/" + stormHomeInZip + "/");
        vargs.add("-Dlogfile.name=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/master.log");
        // vargs.add("-verbose:class");
        vargs.add("com.yahoo.storm.yarn.MasterServer");
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        // Set java executable command
        LOG.info("Setting up app master command:" + vargs);

        amContainer.setCommands(vargs);

        // Set up resource type requirements
        // For now, only memory is supported so we set memory requirements
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(amMB);
        appContext.setResource(capability);
        appContext.setAMContainerSpec(amContainer);

        // submit application
        _yarn.submitApplication(appContext);

        // wait until the app is fully launched
        // waitUntilLaunched();
    }

    /**
     * Wait until the application is successfully launched
     * 
     * @throws IOException
     * @throws YarnException
     */
    public boolean waitUntilLaunched() throws YarnException, IOException {
        while (true) {

            // Check app status every 1 second.
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.debug("Thread sleep in monitoring loop interrupted");
            }

            // Get application report for the appId we are interested in
            ApplicationReport report = _yarn.getApplicationReport(_appId);
            LOG.info("Got application report from ASM for" + ", appId="
                    + _appId.getId() + ", appMasterHost=" + report.getHost()
                    + ", appMasterRpcPort=" + report.getRpcPort()
                    + ", appTrackingUrl=" + report.getTrackingUrl()
                    + ", appUser=" + report.getUser());
            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report
                    .getFinalApplicationStatus();
            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                    LOG.info("Application has completed successfully. Breaking monitoring loop");
                    return true;
                } else {
                    LOG.info("Application did finished unsuccessfully."
                            + " YarnState=" + state.toString()
                            + ", DSFinalStatus=" + dsStatus.toString()
                            + ". Breaking monitoring loop");
                    return false;
                }
            } else if (YarnApplicationState.KILLED == state
                    || YarnApplicationState.FAILED == state) {
                LOG.info("Application did not finish." + " YarnState="
                        + state.toString() + ", DSFinalStatus="
                        + dsStatus.toString() + ". Breaking monitoring loop");
                return false;
            }

            // announce application master's host and port
            if (state == YarnApplicationState.RUNNING) {
                return true;
            }
        }
    }

    /**
     * Find a jar that contains a class of the same name, if any. It will return
     * a jar file, even if that is not the first thing on the class path that
     * has a class with the same name.
     * 
     * @param my_class
     *            the class to find.
     * @return a jar file that contains the class, or null.
     * @throws IOException
     *             on any error
     */
    public static String findContainingJar(Class<?> my_class)
            throws IOException {
        ClassLoader loader = my_class.getClassLoader();
        String class_file = my_class.getName().replaceAll("\\.", "/")
                + ".class";
        for (Enumeration<URL> itr = loader.getResources(class_file); itr
                .hasMoreElements();) {
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

        throw new IOException("Fail to locat a JAR for class: "
                + my_class.getName());
    }

    public static StormOnYarn launchApplication(String appName, String queue,
            int amMB, @SuppressWarnings("rawtypes") Map stormConf,
            String storm_zip_location) throws Exception {
        StormOnYarn storm = new StormOnYarn(stormConf);
        storm.launchApp(appName, queue, amMB, storm_zip_location);
        return storm;
    }

    public static StormOnYarn attachToApp(String appId,
            @SuppressWarnings("rawtypes") Map stormConf) {
        return new StormOnYarn(ConverterUtils.toApplicationId(appId), stormConf);
    }
}

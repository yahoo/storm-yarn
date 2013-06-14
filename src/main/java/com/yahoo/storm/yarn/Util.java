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
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.PathMatcher;
import java.nio.file.SimpleFileVisitor;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.ProtoUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.google.common.base.Joiner;

class Util {
  private static final Logger LOG = LoggerFactory.getLogger(Util.class);
    
  static String getStormHome() {
      String ret = System.getProperty("storm.home");
      if (ret == null) {
        throw new RuntimeException("storm.home is not set");
      }       
      return ret;
  }

  @SuppressWarnings("rawtypes")
  static String getStormVersion(Map conf) throws IOException {
    File releaseFile = new File(getStormHome(), "RELEASE");
    if (releaseFile.exists()) {
      BufferedReader reader = new BufferedReader(new FileReader(releaseFile));
      try {
        return reader.readLine().trim();
      } finally {
        reader.close();
      }
    } else {
      return "Unknown";
    }
  }

  static LocalResource newYarnAppResource(FileSystem fs, Path path,
      LocalResourceType type, LocalResourceVisibility vis) throws IOException {
    Path qualified = fs.makeQualified(path);
    FileStatus status = fs.getFileStatus(qualified);
    LocalResource resource = Records.newRecord(LocalResource.class);
    resource.setType(type);
    resource.setVisibility(vis);
    resource.setResource(ConverterUtils.getYarnUrlFromPath(qualified)); 
    resource.setTimestamp(status.getModificationTime());
    resource.setSize(status.getLen());
    return resource;
  }

  @SuppressWarnings("rawtypes")
  static void rmNulls(Map map) {
    Set s = map.entrySet();
    Iterator it = s.iterator();
    while (it.hasNext()) {
      Map.Entry m =(Map.Entry)it.next();
      if (m.getValue() == null) 
        it.remove();
    }
  }

  @SuppressWarnings("rawtypes")
  static Path createConfigurationFileInFs(FileSystem fs,
          String appHome, Map stormConf) throws IOException {
    // dump stringwriter's content into FS conf/storm.yaml
    Path dirDst = new Path(fs.getHomeDirectory(), appHome + "/conf");
    fs.mkdirs(dirDst);
    Path confDst = new Path(dirDst, "storm.yaml");
    FSDataOutputStream out = fs.create(confDst);
    Yaml yaml = new Yaml();
    OutputStreamWriter writer = new OutputStreamWriter(out);
    rmNulls(stormConf);
    yaml.dump(stormConf, writer);
    writer.close();
    out.close();
    return dirDst;
  } 

  static LocalResource newYarnAppResource(FileSystem fs, Path path)
      throws IOException {
    return Util.newYarnAppResource(fs, path, LocalResourceType.FILE,
        LocalResourceVisibility.APPLICATION);
  }

  static ContainerManager getCMProxy(final YarnRPC rpc,
      ContainerId containerID, final String containerManagerBindAddr,
      ContainerToken containerToken, final Configuration hadoopConf)
      throws IOException {

    final InetSocketAddress cmAddr =
        NetUtils.createSocketAddr(containerManagerBindAddr);
    UserGroupInformation user = UserGroupInformation.getCurrentUser();

    if (UserGroupInformation.isSecurityEnabled()) {
      Token<ContainerTokenIdentifier> token =
          ProtoUtils.convertFromProtoFormat(containerToken, cmAddr);
      // the user in createRemoteUser in this context has to be ContainerID
      user = UserGroupInformation.createRemoteUser(containerID.toString());
      user.addToken(token);
    }

    ContainerManager proxy = user
        .doAs(new PrivilegedAction<ContainerManager>() {
          @Override
          public ContainerManager run() {
            return (ContainerManager) rpc.getProxy(ContainerManager.class,
                cmAddr, hadoopConf);
          }
        });
    return proxy;
  }
  
  private static String getStormConfigPathString() {
      return new File("am-config/storm.yaml").getAbsolutePath();
  }

  @SuppressWarnings("rawtypes")
  private static List<String> buildCommandPrefix(Map conf, String childOptsKey) 
          throws IOException {
      String stormHomePath = getStormHome();
      List<String> toRet = new ArrayList<String>();
      toRet.add("java");
      toRet.add("-server");
      toRet.add("-Dstorm.home=" + stormHomePath);
      toRet.add("-Djava.library.path="
              + conf.get(backtype.storm.Config.JAVA_LIBRARY_PATH));
      toRet.add("-Dstorm.conf.file=" + new
              File(getStormConfigPathString()).getName());
      toRet.add("-cp");
      toRet.add(buildClassPathArgument());

      if (conf.containsKey(childOptsKey)
              && conf.get(childOptsKey) != null) {
          toRet.add((String) conf.get(childOptsKey));
      }

      toRet.add("-Dlogback.configurationFile=" + FileSystems.getDefault()
              .getPath(stormHomePath, "logback", "cluster.xml")
              .toString());

      return toRet;
  }

  @SuppressWarnings("rawtypes")
  static List<String> buildUICommands(Map conf) throws IOException {
      List<String> toRet =
              buildCommandPrefix(conf, backtype.storm.Config.UI_CHILDOPTS);

      toRet.add("-Dlogfile.name=ui.log");
      toRet.add("backtype.storm.ui.core");

      return toRet;
  }

  @SuppressWarnings("rawtypes")
  static List<String> buildNimbusCommands(Map conf) throws IOException {
      List<String> toRet =
              buildCommandPrefix(conf, backtype.storm.Config.NIMBUS_CHILDOPTS);

      toRet.add("-Dlogfile.name=nimbus.log");
      toRet.add("backtype.storm.daemon.nimbus");

      return toRet;
  }

  @SuppressWarnings("rawtypes")
  static List<String> buildSupervisorCommands(Map conf) throws IOException {
      List<String> toRet =
              buildCommandPrefix(conf, backtype.storm.Config.NIMBUS_CHILDOPTS);

      toRet.add("-Dlogfile.name=supervisor.log");
      toRet.add("backtype.storm.daemon.supervisor");

      return toRet;
  }
  
  private static String getConfigPath() {
      return new File("am-config/storm.yaml").getAbsolutePath();
  }

  private static String buildClassPathArgument() throws IOException {
      List<String> paths = new ArrayList<String>();
      paths.add(new File(getConfigPath()).getParent());
      paths.add("./conf/");
      paths.add(getStormHome());
      for (String jarPath : findAllJarsInPaths(getStormHome(),
              getStormHome() + File.separator + "lib")) {
          paths.add(jarPath);
      }
      return Joiner.on(File.pathSeparatorChar).join(paths);
  }

  private static List<String> findAllJarsInPaths(String... pathStrs)
          throws IOException {
      java.nio.file.FileSystem fs = FileSystems.getDefault();
      final PathMatcher matcher = fs.getPathMatcher("glob:**.jar");
      final LinkedHashSet<String> pathSet = new LinkedHashSet<String>();
      for (String pathStr : pathStrs) {
          java.nio.file.Path start = fs.getPath(pathStr);
          Files.walkFileTree(start, new SimpleFileVisitor<java.nio.file.Path>() {
              @Override
              public FileVisitResult visitFile(java.nio.file.Path path,
                      BasicFileAttributes attrs) throws IOException {
                  if (attrs.isRegularFile() && matcher.matches(path)
                          && !pathSet.contains(path)) {
                      java.nio.file.Path parent = path.getParent();
                      pathSet.add(parent + File.separator + "*");
                      return FileVisitResult.SKIP_SIBLINGS;
                  }
                  return FileVisitResult.CONTINUE;
              }
          });
      }
      final List<String> toRet = new ArrayList<String>();
      for (String p : pathSet) {
          toRet.add(p);
      }
      return toRet;
  }
}

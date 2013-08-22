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

import java.net.URI;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

import java.io.OutputStreamWriter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.yaml.snakeyaml.Yaml;

import com.google.common.base.Joiner;

class Util {

  private static final String STORM_CONF_PATH_STRING = 
      "conf" + Path.SEPARATOR + "storm.yaml";
    
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

  static String getStormHomeInZip(FileSystem fs, Path zip, String stormVersion) throws IOException, RuntimeException {
    FSDataInputStream fsInputStream = fs.open(zip);
    ZipInputStream zipInputStream = new ZipInputStream(fsInputStream);
    ZipEntry entry = zipInputStream.getNextEntry();
    while (entry != null) {
      String entryName = entry.getName();
      if (entryName.matches("^storm(-" + stormVersion + ")?/")) {
        fsInputStream.close();
        return entryName.replace("/", "");
      }
      entry = zipInputStream.getNextEntry();
    }
    fsInputStream.close();
    throw new RuntimeException("Can not find storm home entry in storm zip file.");
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
          String appHome, Map stormConf, YarnConfiguration yarnConf) 
          throws IOException {
    // dump stringwriter's content into FS conf/storm.yaml
    Path confDst = new Path(fs.getHomeDirectory(),
            appHome + Path.SEPARATOR + STORM_CONF_PATH_STRING);
    Path dirDst = confDst.getParent();
    fs.mkdirs(dirDst);
    
    //storm.yaml
    FSDataOutputStream out = fs.create(confDst);
    Yaml yaml = new Yaml();
    OutputStreamWriter writer = new OutputStreamWriter(out);
    rmNulls(stormConf);
    yaml.dump(stormConf, writer);
    writer.close();
    out.close();

    //yarn-site.xml
    Path yarn_site_xml = new Path(dirDst, "yarn-site.xml");
    out = fs.create(yarn_site_xml);
    writer = new OutputStreamWriter(out);
    yarnConf.writeXml(writer);
    writer.close();
    out.close();   
    
    return dirDst;
  } 

  static LocalResource newYarnAppResource(FileSystem fs, Path path)
      throws IOException {
    return Util.newYarnAppResource(fs, path, LocalResourceType.FILE,
        LocalResourceVisibility.APPLICATION);
  }

  @SuppressWarnings("rawtypes")
  private static List<String> getCommonJavaParameters(Map conf, String childOptsKey) 
          throws IOException {
      String stormHomePath = getStormHome();
      List<String> toRet = new ArrayList<String>();
      toRet.add("-server");
      toRet.add("-Dstorm.home=" + stormHomePath);
      toRet.add("-Djava.library.path="
              + conf.get(backtype.storm.Config.JAVA_LIBRARY_PATH));
      toRet.add("-Dstorm.conf.file=" + new
              File(STORM_CONF_PATH_STRING).getName());
      toRet.add("-cp");
      toRet.add(buildClassPathArgument());

      if (conf.containsKey(childOptsKey)
              && conf.get(childOptsKey) != null) {
          toRet.add((String) conf.get(childOptsKey));
      }

      File stormPath = new File(stormHomePath);
      File logBackDir = new File(stormPath, "logback");
      File logBackConfig = new File(logBackDir, "cluster.xml");     
      
      toRet.add("-Dlogback.configurationFile=" + logBackConfig.toString());

      return toRet;
  }

  @SuppressWarnings("rawtypes")
  static List<String> buildUICommands(Map conf) throws IOException {
      List<String> toRet = getCommonJavaParameters(conf, backtype.storm.Config.UI_CHILDOPTS);
      File jvm = new File(new File(System.getProperty("java.home"), "bin"), "java");
      toRet.add(0, jvm.toString());

      toRet.add("-Dlogfile.name=ui.log");
      toRet.add("backtype.storm.ui.core");

      return toRet;
  }

  @SuppressWarnings("rawtypes")
  static List<String> buildNimbusCommands(Map conf) throws IOException {
      List<String> toRet = getCommonJavaParameters(conf, backtype.storm.Config.NIMBUS_CHILDOPTS);
      File jvm = new File(new File(System.getProperty("java.home"), "bin"), "java");
      toRet.add(0, jvm.toString());
      
      toRet.add("-Dlogfile.name=nimbus.log");
      toRet.add("backtype.storm.daemon.nimbus");

      return toRet;
  }

  @SuppressWarnings("rawtypes")
  static List<String> buildSupervisorCommands(Map conf) throws IOException {
      List<String> toRet = getCommonJavaParameters(conf, backtype.storm.Config.NIMBUS_CHILDOPTS);
      toRet.add(0, "$JAVA_HOME/bin/java");
      
      toRet.add("-Dlogfile.name=supervisor.log");
      toRet.add("backtype.storm.daemon.supervisor");
      toRet.add(0, "cat `basename $0` >> /var/log/hadoop-yarn/containers/container.log; ");
      toRet.add("2>&1 | tee -a /var/log/hadoop-yarn/containers/container.log; test ${PIPESTATUS[0]} -eq 0");

      return toRet;
  }

    private static String buildClassPathArgument() throws IOException {
        List<String> paths = new ArrayList<String>();
        paths.add(new File(STORM_CONF_PATH_STRING).getParent());
        paths.add(getStormHome());
        for (String jarPath : findAllJarsInPaths(getStormHome(),
                getStormHome() + File.separator + "lib")) {
            paths.add(jarPath);
        }
        return Joiner.on(File.pathSeparatorChar).join(paths);
    }

    private static interface FileVisitor {
        public void visit(File file);
    }
  
    private static List<String> findAllJarsInPaths(String... pathStrs) {
        final LinkedHashSet<String> pathSet = new LinkedHashSet<String>();

        FileVisitor visitor = new FileVisitor() {

            @Override
            public void visit(File file) {
                String name = file.getName();
                if (name.endsWith(".jar")) {
                    pathSet.add(file.getPath());
                }
            }
        };

        for (String path : pathStrs) {
            File file = new File(path);
            traverse(file, visitor);
        }

        final List<String> toRet = new ArrayList<String>();
        for (String p : pathSet) {
            toRet.add(p);
        }
        return toRet;
    }

    private static void traverse(File file, FileVisitor visitor) {
        if (file.isDirectory()) {
            File childs[] = file.listFiles();
            if (childs.length > 0) {
                for (int i = 0; i < childs.length; i++) {
                    File child = childs[i];
                    traverse(child, visitor);
                }
            }
        } else {
            visitor.visit(file);
        }
    }

    static String getApplicationHomeForId(String id) {
        if (id.isEmpty()) {
            throw new IllegalArgumentException(
                    "The ID of the application cannot be empty.");
        }
        return ".storm" + Path.SEPARATOR + id;
    }

    /**
     * Returns a boolean to denote whether a cache file is visible to all(public)
     * or not
     * @param fs  Hadoop file system
     * @param path  file path
     * @return true if the path is visible to all, false otherwise
     * @throws IOException
     */
    static boolean isPublic(FileSystem fs, Path path) throws IOException {
        //the leaf level file should be readable by others
        if (!checkPermissionOfOther(fs, path, FsAction.READ)) {
            return false;
        }
        return ancestorsHaveExecutePermissions(fs, path.getParent());
    }

    /**
     * Checks for a given path whether the Other permissions on it
     * imply the permission in the passed FsAction
     * @param fs
     * @param path
     * @param action
     * @return true if the path in the uri is visible to all, false otherwise
     * @throws IOException
     */
    private static boolean checkPermissionOfOther(FileSystem fs, Path path,
                                                  FsAction action) throws IOException {
        FileStatus status = fs.getFileStatus(path);
        FsPermission perms = status.getPermission();
        FsAction otherAction = perms.getOtherAction();
        if (otherAction.implies(action)) {
            return true;
        }
        return false;
    }

    /**
     * Returns true if all ancestors of the specified path have the 'execute'
     * permission set for all users (i.e. that other users can traverse
     * the directory hierarchy to the given path)
     */
    static boolean ancestorsHaveExecutePermissions(FileSystem fs, Path path) throws IOException {
        Path current = path;
        while (current != null) {
            //the subdirs in the path should have execute permissions for others
            if (!checkPermissionOfOther(fs, current, FsAction.EXECUTE)) {
                return false;
            }
            current = current.getParent();
        }
        return true;
    }
    
    static void redirectStreamAsync(final InputStream input, final PrintStream output) {
      new Thread(new Runnable() {        
          @Override
          public void run() {
              Scanner scanner = new Scanner(input);
              while (scanner.hasNextLine()) {
                  output.println(scanner.nextLine());
              }
          }
      }).start();
  }
}

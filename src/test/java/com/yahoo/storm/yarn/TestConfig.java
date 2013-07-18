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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.yaml.snakeyaml.Yaml;

public class TestConfig {
    private static final Log LOG = LogFactory.getLog(TestConfig.class);

    private File yarn_site_xml;
    private File storm_conf_file;
    private String storm_home = null;

    synchronized File createYarnSiteConfig(Configuration yarn_conf) throws IOException {
        yarn_site_xml = new File("./target/conf/yarn-site.xml");
        yarn_site_xml.getParentFile().mkdirs();
        FileWriter writer = new FileWriter(yarn_site_xml);
        yarn_conf.writeXml(writer);
        writer.flush();
        writer.close();   
        return yarn_site_xml;
    }

    @SuppressWarnings("rawtypes")
    synchronized File createConfigFile(Map storm_conf) throws IOException {
        storm_conf_file = new File("./conf/storm.yaml");
        storm_conf_file.getParentFile().mkdirs();
        Yaml yaml = new Yaml();
        FileWriter writer = new FileWriter(storm_conf_file);
        Util.rmNulls(storm_conf);
        yaml.dump(storm_conf, writer);
        writer.flush();
        writer.close();
        return storm_conf_file;
    }

    void cleanup() {
        if (storm_conf_file != null) 
            deleteFolder(storm_conf_file.getParentFile());
        deleteFolder(new File(storm_home));
    }

    synchronized String stormHomePath() throws IOException {
        unzipFile("./lib/storm.zip");
        Runtime.getRuntime().exec( "chmod +x "+storm_home+"/bin/storm");
        System.setProperty("storm.home", storm_home);
        return storm_home;
    }

    private void unzipFile(String filePath){
        FileInputStream fis = null;
        ZipInputStream zipIs = null;
        ZipEntry zEntry = null;
        try {
            fis = new FileInputStream(filePath);
            zipIs = new ZipInputStream(new BufferedInputStream(fis));
            while((zEntry = zipIs.getNextEntry()) != null){
                try{
                    byte[] tmp = new byte[4*1024];
                    FileOutputStream fos = null;
                    String opFilePath = "lib/"+zEntry.getName();
                    if (zEntry.isDirectory()) { 
                        LOG.debug("Create a folder "+opFilePath);
                        if (zEntry.getName().indexOf(Path.SEPARATOR)==(zEntry.getName().length()-1))
                            storm_home = opFilePath.substring(0, opFilePath.length()-1);
                        new File(opFilePath).mkdir();
                    } else {
                        LOG.debug("Extracting file to "+opFilePath);
                        fos = new FileOutputStream(opFilePath);
                        int size = 0;
                        while((size = zipIs.read(tmp)) != -1){
                            fos.write(tmp, 0 , size);
                        }
                        fos.flush();
                        fos.close();
                    }
                } catch(Exception ex){

                }
            }
            zipIs.close();
        } catch (FileNotFoundException e) {
            LOG.warn(e.toString());
        } catch (IOException e) {
            LOG.warn(e.toString());
        }
    }

    private void deleteFolder(File file) {
        if (file.isDirectory()) {
            //directory is empty, then delete it
            if(file.list().length==0){
                file.delete();
            } else {
                //list all the directory contents
                String files[] = file.list();
                for (String temp : files) {
                    //construct the file structure
                    File fileDelete = new File(file, temp);
                    //recursive delete
                    deleteFolder(fileDelete);
                }

                //check the directory again, if empty then delete it
                if(file.list().length==0){
                    file.delete();
                }
            }
        } else {
            //if file, then delete it
            file.delete();
        }
    }
}

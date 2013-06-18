package com.yahoo.storm.yarn;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.yaml.snakeyaml.Yaml;

public class TestConfig {
    private static final Log LOG = LogFactory.getLog(TestConfig.class);

    @SuppressWarnings("rawtypes")
    static File createConfigFile(Map storm_conf) throws IOException {
        File storm_conf_file = new File("./conf/storm.yaml");
        storm_conf_file.getParentFile().mkdirs();
        Yaml yaml = new Yaml();
        FileOutputStream out = new FileOutputStream(storm_conf_file);
        OutputStreamWriter writer = new OutputStreamWriter(out);
        Util.rmNulls(storm_conf);
        yaml.dump(storm_conf, writer);
        writer.close();
        out.close();   
        return storm_conf_file;
    }

    static void rmConfigFile(File storm_conf_file) {
        if (storm_conf_file != null) {
            storm_conf_file.delete();
            storm_conf_file.getParentFile().deleteOnExit();
        }

        deleteFolder(new File("./lib/storm"));
    }

    static String stormHomePath() throws IOException {
        unzipFile("./lib/storm.zip");
        String storm_home = "./lib/storm";
        System.setProperty("storm.home", storm_home);
        return storm_home;
    }

    private static void unzipFile(String filePath){
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

    private static void deleteFolder(File file) {
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

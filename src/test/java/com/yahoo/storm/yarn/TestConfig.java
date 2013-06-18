package com.yahoo.storm.yarn;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

public class TestConfig {
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
    }

    static String stormHomePath() throws IOException {
        String pathEnvString = System.getenv().get("PATH");
        for (String pathStr : pathEnvString.split(File.pathSeparator)) {
            // Is storm binary located here?  Path start = fs.getPath(pathStr);
            File f = new File(pathStr);
            if (f.isDirectory()) {
                File[] files = f.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        if (name.equals("storm")) {
                            return true;
                        }
                        return false;
                    }
                });
                if (files.length > 0) {
                    File canonicalPath = new File(pathStr + File.separator +
                            "storm").getCanonicalFile();
                    String storm_home = (canonicalPath.getParentFile().getParent());
                    System.setProperty("storm.home", storm_home);
                    return storm_home;
                }
            }
        }

        return null;
    }


}

package com.yahoo.storm.yarn.client;

import org.apache.hadoop.yarn.api.ApplicationConstants;

public class Constants {
    /**                                                                                                                                                                                                       
     * Default CLASSPATH for YARN applications. A comma-separated list of                                                                                                                                     
     * CLASSPATH entries                                                                                                                                                                                      
     */
    public static final String[] DEFAULT_YARN_APPLICATION_CLASSPATH = {
        ApplicationConstants.Environment.HADOOP_CONF_DIR.$(),
        ApplicationConstants.Environment.HADOOP_COMMON_HOME.$()
            + "/share/hadoop/common/*",
        ApplicationConstants.Environment.HADOOP_COMMON_HOME.$()
            + "/share/hadoop/common/lib/*",
        ApplicationConstants.Environment.HADOOP_HDFS_HOME.$()
            + "/share/hadoop/hdfs/*",
        ApplicationConstants.Environment.HADOOP_HDFS_HOME.$()
            + "/share/hadoop/hdfs/lib/*",
        ApplicationConstants.Environment.YARN_HOME.$()
            + "/share/hadoop/yarn/*",
        ApplicationConstants.Environment.YARN_HOME.$()
            + "/share/hadoop/yarn/lib/*" };
    
    /**
     * Value used to define no locality
     */
    static final String ANY = "*";
}

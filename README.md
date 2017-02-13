<!--
  Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

storm-yarn
=================
Storm-yarn enables Storm clusters to be deployed into machines managed by Hadoop YARN. It is still a work in progress.

## Contributors

* Andy Feng ([@anfeng](https://github.com/anfeng))
* Robert Evans ([@revans2](https://github.com/revans2))
* Derek Dagit ([@d2r](https://github.com/d2r))
* Nathan Roberts ([@ynroberts](https://github.com/ynroberts))
* Xin Wang ([@vesense](https://github.com/vesense))

## Mailing list

Feel free to ask questions on storm-yarn's mailing list: http://groups.google.com/group/storm-yarn

## New Features:

Based on the project developed by yahoo, we have added following new features.

1. We have updated the version of storm from 0.9.0 to 1.0.1.

2. We have added StormClusterChecker class, and this class is able to monitor the storm cluster. It can adjust the number of supervisor based on usage of system resources.

3. We have added the function of removeSupervisors in order to monitor resources. Its function be opposite to addSupervisors.

4. We have updated the logging framework from logback to log4j2.

## How to install and use

Before starting, please make sure you have Hadoop YARN running.

Besides, The storm-on-yarn implementation does not include running Zookeeper on YARN. You need to make sure you have available Zookeeper service. 

When you finish the work above, you can beginning to detailed installation and use.

The detailed installation including download, build, deployment and running.

### Download and build

Please install the Java 8 and Maven 3 first. These two software are necessary to complied and packaged source code of storm-on-yarn.

You can download the source code of storm-on-yarn or use the command ``git clone <link> `` to get the source code.

After you download the source code, you need edit pom.xml in storm-on-yarn root directory to set the Hadoop version.

![pom.xml](https://github.com/wendyshusband/storm-yarn/blob/storm-1.0.1/image/editpom.png)

To packaged items ,You execute the following command in storm-on-yarn root directory.

    mvn package

You will see that storm-yarn commands being executed.
<pre><code>17:57:27.810 [main] INFO  com.yahoo.storm.yarn.TestIntegration - bin/storm-yarn launch ./conf/storm.yaml --stormZip lib/storm.zip --appname storm-on-yarn-test --output target/appId.txt
17:57:59.681 [main] INFO  com.yahoo.storm.yarn.TestIntegration - bin/storm-yarn getStormConfig ./conf/storm.yaml --appId application_1372121842369_0001 --output ./lib/storm/storm.yaml
17:58:04.382 [main] INFO  com.yahoo.storm.yarn.TestIntegration - ./lib/storm/bin/storm jar lib/storm-starter-0.0.1-SNAPSHOT.jar storm.starter.ExclamationTopology exclamation-topology
17:58:04.382 [main] INFO  com.yahoo.storm.yarn.TestIntegration - ./lib/storm/bin/storm kill exclamation-topology
17:58:07.798 [main] INFO  com.yahoo.storm.yarn.TestIntegration - bin/storm-yarn stopNimbus ./conf/storm.yaml --appId application_1372121842369_0001
17:58:10.131 [main] INFO  com.yahoo.storm.yarn.TestIntegration - bin/storm-yarn startNimbus ./conf/storm.yaml --appId application_1372121842369_0001
17:58:12.460 [main] INFO  com.yahoo.storm.yarn.TestIntegration - bin/storm-yarn stopUI ./conf/storm.yaml --appId application_1372121842369_0001
17:58:15.045 [main] INFO  com.yahoo.storm.yarn.TestIntegration - bin/storm-yarn startUI ./conf/storm.yaml --appId application_1372121842369_0001
17:58:17.390 [main] INFO  com.yahoo.storm.yarn.TestIntegration - bin/storm-yarn shutdown ./conf/storm.yaml --appId application_1372121842369_0001
</code></pre>

If you want to skip the tests you can run

    mvn package -DskipTests

### Deploy:

After you build the whole project of storm-on-yarn, you need install storm-on-yarn and Storm on the Storm Client machine.

Storm Client machine refers to the machine that will submit the YARN application to RM.

You need copy packaged storm-on-yarn project to Storm Client machine, downloading the project of [storm-1.0.1](http://www.apache.org/dyn/closer.lua/storm/apache-storm-1.0.1/apache-storm-1.0.1.tar.gz). and put the decompressed project of storm-1.0.1 into same directory as the storm-on-yarn root directory.

As shown below,

![stormHome](https://github.com/wendyshusband/storm-yarn/blob/storm-1.0.1/image/stormhome.png)

You do not need to start running the Storm cluster, as this will be done by running storm-on-yarn later on.

When executing storm-on-yarn commands, the commands “storm-yarn”, “storm”, etc. will be called. Therefore, all paths to the bin files containing these executable commands must be included to the PATH environment variable.

Hence you add storm-1.0.1/bin and $(storm-on-yarn root directory)/bin to your PATH environment variable.

![environment](https://github.com/wendyshusband/storm-yarn/blob/storm-1.0.1/image/environment.png)

Storm-on-yarn will deploy a copy of Storm code throughout all the nodes of the YARN cluster using HDFS. However, the location of where to fetch this copy of the Storm code is hard-coded into the Storm-on-YARN client. Therefore,  you will have to manually prepare the copy inside HDFS.

The storm.zip copy needs to be stored in “/lib/storm/<storm version>/storm.zip” path in HDFS. 

Besides, you shall put the dependencies of your job into $(storm-on-yarn root directory)/lib.

After you finish the work above, you can use the following commands to copy the storm.zip from the local directory to “/lib/storm/1.0.1” in HDFS.

    hadoop fs -mkdir /lib 
    hadoop fs -mkdir /lib/storm
    hadoop fs -mkdir /lib/storm/1.0.1
    zip -r storm.zip storm-1.0.1
    hadoop fs -put storm.zip /lib/storm/1.0.1/

### Run:

Everything should be ready. You can start to run the storm-on-yarn project.

The storm-yarn command provides a way to launch a storm cluster. In the future
it is intended to also provide ways to manage the cluster.

To launch a cluster you can run

    storm-yarn launch <storm-yarn-configuration>

storm-yarn-configuration (a yaml file) will be used to launch a Storm cluster.

In this project, we provide two quick ways to create the storm-yarn-configuration file:

a)    Edit storm.yaml that under storm-1.0.1/conf.

b)    Copy the $(storm-on-yarn root directory)/src/main/master_defaults.yaml to storm-1.0.1/conf and rename it to master.yaml. And you can edit it to configuration.

The simplest, you only need to add zookeeper cluster information.

For example:
      using a) method:

![Configuration](https://github.com/wendyshusband/storm-yarn/blob/storm-1.0.1/image/config.png)

storm-yarn has a number of new config options to configure the storm AM.
   * master.initial-num-supervisors is the number of supervisors to launch with storm.
   * master.container.size-mb is the size of the container to request.
"storm-yarn launch" produces an Application ID, which identify the newly launched Storm master.
This Application ID should be used for accessing the Storm master.

To obtain a storm.yaml from the newly launch Storm master, you can run

    storm-yarn getStormConfig <storm-yarn-config> --appId <Application-ID> --output <storm.yaml>

storm.yaml will be retrieved from Storm master.  

After storing the above storm.yaml in Storm classpath (ex. ~/.storm/storm.yaml), you will invoke standard Storm commands against the Storm cluster on YARN. For example, you run the following command to submit a topology

    storm jar <appJar>

For a full list of storm-yarn commands and options you can run

    storm-yarn help

Note:storm-on-yarn is now configured to use Netty for communication between spouts and bolts.

It's pure JVM based, and thus OS independent.

If you are running storm using zeromq (instead of Netty), you need to augment the standard storm.zip file the needed .so files. This can be done with the not ideally named create-tarball.sh script

    create-tarball.sh storm.zip

Ideally the storm.zip file is a world readable file installed by ops so there is
only one copy in the distributed cache ever.

## Commands:

| Command     | Function|
| --------    | ------  |
| launch      | launch storm on yarn|
| help        | get help|
| version     | view storm version|
| addSupervisors/removeSupervisor       | add/remove supervisor|
| startNimbus/stopNimbus    | start/stop Nimbus|
| startUI/stopUI        | start/stop Web UI|
| startSupervisors/stopSupervisor   | start or stop all supervisors|
| shutdown    | shutdown storm cluster|
##  Arguments：

| Argument     | Function|
| --------    | ------  |
| -appname <arg>      | (Only for storm-yarn launch)Application Name.Default value – Storm-on-Yarn|
| -appId <arg>        | (Required) The storm clusters app ID|
| -output <arg>     | Output file|
| -supervisor <arg>       | (Required for removeSupervisors) the supervisor to be removed|
| -supervisors <arg>    | (Required for addSupervisors) The # of supervisors to be added|

## Known Issues:

There is no failover when nimbus goes down. Still working on it.

There is no simple way to get to the logs for the different processes.

## License

The use and distribution terms for this software are covered by the
Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0.html).


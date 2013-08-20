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

We intend to support JDK6 in this repo.
==============


storm-yarn
=================
Storm-yarn enables Storm clusters to be deployed into machines managed by Hadoop YARN.  It is still a work
in progress.


## Contributors

* Andy Feng ([@anfeng](https://github.com/anfeng))
* Robert Evans ([@revans2](https://github.com/revans2))
* Derek Dagit ([@d2r](https://github.com/d2r))
* Nathan Roberts ([@ynroberts](https://github.com/ynroberts))

## Mailing list

Feel free to ask questions on storm-yarn's mailing list: http://groups.google.com/group/storm-yarn

## Prerequisite

Please install the following software first:
   * Java 7
   
## Build

To run the tests,  you execute the following command. 

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

## Deploy:

You need to install a version of storm on the hadoop gateway.

You also need to place a corresponding storm.zip file in HDFS so it can be
shipped to all of the nodes through the distributed cache at

/lib/storm/&lt;storm-version&gt;/storm.zip

Storm-YARN is now configured to use Netty for communication between spouts and bolts.
It's pure JVM based, and thus OS independent.

If you are running storm using zeromq (instead of Netty), you need to augment the standard
storm.zip file the needed .so files. This can be done with the not ideally
named create-tarball.sh script

    create-tarball.sh storm.zip

Ideally the storm.zip file is a world readable file installed by ops so there is
only one copy in the distributed cache ever.

## Run:

The yarn-storm command provides a way to launch a storm cluster.  In the future
it is intended to also provide ways to manage the cluster.

To launch a cluster you can run

    storm-yarn launch <storm-yarn-config>

storm-yarn-configuration (a yaml file) will be used to launch a Storm cluster.
storm-yarn has a number of new config options to configure the storm AM.
   * master.initial-num-supervisors is the number of supervisors to launch with storm.
   * master.container.size-mb is the size of the container to request.
"storm-yarn launch" produces an Application ID, which identify the newly launched Storm master.
This Application ID should be used for accessing the Storm master.

To obtain a storm.yaml from the newly launch Storm master, you can run

    storm-yarn getStormConfig <storm-yarn-config> --appId <Application-ID> --output <storm.yaml>

storm.yaml will be retrieved from Storm master.  

After storing the above storm.yaml in Storm classpath (ex. ~/.storm/storm.yaml), you will 
invoke standard Storm commands against the Storm cluster on YARN. For example, you run 
the following command to submit a topology

    storm jar <appJar>

For a full list of storm-yarn commands and options you can run

    storm-yarn help

## Known Issues:

The is no failover when nimbus goes down. Still working on it.

There is no simple way to get to the logs for the different processes.

## License

The use and distribution terms for this software are covered by the
Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0.html).


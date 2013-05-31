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
storm-yarn-master
=================
storm on yarn application master

This is to provide basic storm on YARN functionality.  It is still a work
in progress.


## Contributors

* Andy Feng ([@anfeng](https://github.com/anfeng))
* Robert Evans ([@revans2](https://github.com/revans2))
* Derek Dagit ([@d2r](https://github.com/d2r))
* Nathan Roberts ([@ynroberts](https://github.com/ynroberts))

## Build:

To run the tests you need an instance of storm installed and reachable on your PATH.

    mvn package

If you want to skip the tests you can run

    mvn pacakge -DskipTests

## Deploy:

You need to install a version of storm on the hadoop gateway.

You also need to place a corresponding storm.zip file in HDFS so it can be
shipped to all of the nodes through the distributed cache at

/lib/storm/&lt;storm-version&gt;/storm.zip

If you are running storm using zeromq and jzmq you need to augment the standard
storm.zip file the needed .so files. This can be done with the not ideally
named create-tarball.sh script

    create-tarball.sh storm.zip

Ideally the storm.zip file is a world readable file installed by ops so there is
only one copy in the distributed cache ever.

## Run:

The yarn-storm command provides a way to launch a storm cluster.  In the future
it is intended to also provide ways to manage the cluster.

To launch a cluster you can run

    yarn-storm launch

It will launch a cluster using what is currently in your storm.yaml.

For a full list of commands and options you can run

    yarn-storm help

There are a number of new config options to configure the storm AM.

master.initial-num-supervisors is the number of supervisors to launch with storm.
master.container.size-mb is the size of the container to request.

These are typically stored in a master.yaml instead of a storm.yaml file.

## Known Issues:

The is no failover when nimbus goes down. Still working on it.

There is no easy way to get the normal storm client to connect to the 
nimbus server that was just launched.  (You need to find the host and port
and then configure it automatically on the command line.)

The default UI port is the same as the default MR shuffle port.  You
probably want to change that to avoid collisions.

There is no simple way to get to the logs for the different processes.

## License

The use and distribution terms for this software are covered by the
Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0.html).


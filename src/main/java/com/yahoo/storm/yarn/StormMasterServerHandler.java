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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.List;
import java.util.Map;

import org.apache.thrift7.TException;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.google.common.base.Joiner;
import com.yahoo.storm.yarn.generated.StormMaster;

public class StormMasterServerHandler implements StormMaster.Iface {
    private static final Logger LOG = LoggerFactory.getLogger(StormMasterServerHandler.class);
    @SuppressWarnings("rawtypes")
    Map _storm_conf;
    StormAMRMClient _client;
    
    StormMasterServerHandler(@SuppressWarnings("rawtypes") Map storm_conf, StormAMRMClient client) {
        _storm_conf = storm_conf;
        Util.rmNulls(_storm_conf);
        _client = client;
    }
    
    @Override
    public String getStormConf() throws TException {
        LOG.info("getStormConf");
        return JSONValue.toJSONString(_storm_conf);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setStormConf(String storm_conf) throws TException {
        Object json = JSONValue.parse(storm_conf);
        if (!json.getClass().isAssignableFrom(Map.class)) {
            LOG.warn("Could not parse configuration into a Map: " + storm_conf);
            return;
        }
        Map<?, ?> new_conf = (Map<?, ?>)json;
        _storm_conf.putAll(new_conf);
        Util.rmNulls(_storm_conf);

        // TODO Handle supervisor config change.
        stopNimbus();
        stopUI();
        startNimbus();
        startUI();
    }

    @Override
    public void addSupervisors(int number) throws TException {
        _client.addSupervisors(number);
    }
    
    class StormProcess extends Thread{
    	Process _process;
    	String _name;
    	
    	public StormProcess(String name){
    		_name = name;
    	}
    	
	public void run(){
		startStormProcess();
		try {
			_process.waitFor();
			LOG.info("Storm process "+_name+" stopped");
		} catch (InterruptedException e) {
                        LOG.info("Interrupted => will stop the storm process too");
			_process.destroy();
                }
	}
	
	void writeConfigFile() throws IOException {
            File configFile = new File("am-config/storm.yaml").getAbsoluteFile();
            configFile.getParentFile().mkdir();
            Yaml yaml = new Yaml();
            yaml.dump(_storm_conf, new FileWriter(configFile));
	}

        private void startStormProcess() {
            try {
                writeConfigFile();
                LOG.info("Running: " + Joiner.on(" ").join(buildCommands()));
                ProcessBuilder builder =
                        new ProcessBuilder(buildCommands())
                        .redirectError(Redirect.INHERIT)
                        .redirectOutput(Redirect.INHERIT);

                _process = builder.start();
            } catch (IOException e) {
                LOG.warn("Error starting nimbus process ", e);
            }
        }
        
        private List<String> buildCommands() throws IOException {
            if (_name == "nimbus") {
                return Util.buildNimbusCommands(_storm_conf);
            } else if (_name == "ui") {
                return Util.buildUICommands(_storm_conf);
            }

            throw new IllegalArgumentException(
                    "Cannot build command list for \"" + _name + "\"");
        }

        public void stopStormProcess() {
            _process.destroy();
        }
    }
    
    StormProcess nimbusProcess = new StormProcess("nimbus");
    StormProcess uiProcess = new StormProcess("ui");
    
    @Override
    public void startNimbus() {
	synchronized(this) {
		if (nimbusProcess.isAlive()){
			LOG.info("Received a request to start nimbus, but it is running now");
			return;
		}
        	LOG.info("Starting nimbus...");
       		nimbusProcess.start(); 
	}       
    }

    @Override
    public void stopNimbus() {
	synchronized(this) {
                if (!nimbusProcess.isAlive()){
			LOG.info("Received a request to stop nimbus, but it is not running now");
			return;
		}
    		LOG.info("Stoping nimbus...");
    		nimbusProcess.stopStormProcess();
	}
    }

    @Override
    public void startUI() throws TException {
    	synchronized(this) {
		if (uiProcess.isAlive()){
                	LOG.info("Received a request to start UI, but it is running now");
                	return;
        	}
		LOG.info("Starting ui...");
    		uiProcess.start();
	} 
    }

    @Override
    public void stopUI() throws TException {
    	synchronized(this) {
                if (!uiProcess.isAlive()){
                        LOG.info("Received a request to stop UI, but it is not running now");
                        return;
                }
                LOG.info("Stoping ui...");
                uiProcess.stopStormProcess();
        }
    }

    @Override
    public void startSupervisors() throws TException {
        _client.startAllSupervisors();
    }

    @Override
    public void stopSupervisors() throws TException {
        _client.stopAllSupervisors();
    }
}

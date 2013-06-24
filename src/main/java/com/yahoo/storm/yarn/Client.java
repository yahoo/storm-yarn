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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    public static interface ClientCommand {

        /**
         * @return the options this client will process.
         */
        public Options getOpts();

        /**
         * Do the processing
         * @param cl the arguments to process
         * @param stormConf the storm configuration to use
         * @throws Exception on any error
         */
        public void process(CommandLine cl,
                @SuppressWarnings("rawtypes") Map stormConf) throws Exception;
    }

    public static class HelpCommand implements ClientCommand {
        HashMap<String, ClientCommand> _commands;
        public HelpCommand(HashMap<String, ClientCommand> commands) {
            _commands = commands;
        }

        @Override
        public Options getOpts() {
            return new Options();
        }

        @SuppressWarnings("unchecked")
        @Override
        public void process(CommandLine cl,
                @SuppressWarnings("rawtypes") Map ignored) throws Exception {
            printHelpFor(cl.getArgList());
        }

        public void printHelpFor(Collection<String> args) {
            if(args == null || args.size() < 1) {
                args = _commands.keySet();
            }
            HelpFormatter f = new HelpFormatter();
            for(String command: args) {
                ClientCommand c = _commands.get(command);
                if (c != null) {
                    //TODO Show any arguments to the commands.
                    f.printHelp(command, c.getOpts());
                } else {
                    System.err.println("ERROR: " + c + " is not a supported command.");
                    //TODO make this exit with an error at some point
                }
            }
        }
    }
    
    /**
     * @param args the command line arguments
     * @throws Exception  
     */    
    @SuppressWarnings("rawtypes")
    public void execute(String[] args) throws Exception {
        HashMap<String, ClientCommand> commands = new HashMap<String, ClientCommand>();
        HelpCommand help = new HelpCommand(commands);
        commands.put("help", help);
        commands.put("launch", new LaunchCommand());
        commands.put("setStormConfig", new StormMasterCommand(StormMasterCommand.COMMAND.SET_STORM_CONFIG));
        commands.put("getStormConfig", new StormMasterCommand(StormMasterCommand.COMMAND.GET_STORM_CONFIG));
        commands.put("addSupervisors", new StormMasterCommand(StormMasterCommand.COMMAND.ADD_SUPERVISORS));
        commands.put("startNimbus", new StormMasterCommand(StormMasterCommand.COMMAND.START_NIMBUS));
        commands.put("stopNimbus", new StormMasterCommand(StormMasterCommand.COMMAND.STOP_NIMBUS));
        commands.put("startUI", new StormMasterCommand(StormMasterCommand.COMMAND.START_UI));
        commands.put("stopUI", new StormMasterCommand(StormMasterCommand.COMMAND.STOP_UI));
        commands.put("startSupervisors", new StormMasterCommand(StormMasterCommand.COMMAND.START_SUPERVISORS));
        commands.put("stopSupervisors", new StormMasterCommand(StormMasterCommand.COMMAND.STOP_SUPERVISORS));
        commands.put("shutdown", new StormMasterCommand(StormMasterCommand.COMMAND.SHUTDOWN));

        String commandName = null;
        String[] commandArgs = null;
        if (args.length < 1) {
            commandName = "help";
            commandArgs = new String[0];
        } else {
            commandName = args[0];
            commandArgs = Arrays.copyOfRange(args, 1, args.length);
        }
        ClientCommand command = commands.get(commandName);
        if(command == null) {
            LOG.error("ERROR: " + commandName + " is not a supported command.");
            help.printHelpFor(null);
            System.exit(1);
        }
        Options opts = command.getOpts();
        if(!opts.hasOption("h")) {
            opts.addOption("h", "help", false, "print out a help message");
        }
        CommandLine cl = new GnuParser().parse(command.getOpts(), commandArgs);
        if(cl.hasOption("help")) {
            help.printHelpFor(Arrays.asList(commandName));
        } else {
            String config_file = null;
            if (!commandName.equals("help")) {
                List remaining_args = cl.getArgList();
                if (remaining_args!=null && !remaining_args.isEmpty())
                    config_file = (String)remaining_args.get(0);
            }
            Map storm_conf = Config.readStormConfig(config_file);
            command.process(cl, storm_conf);
        }
    }

    public static void main(String[] args) throws Exception {
        Client client = new Client();
        client.execute(args);
    } 
}

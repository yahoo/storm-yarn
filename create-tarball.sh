#!/bin/bash
#  Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#

function set_LibraryPaths() {
    # This is hard-coded from storm.
    LibraryPaths="/usr/local/lib:/opt/local/lib:/usr/lib"

    local readonly LDLibraryPaths="$(echo "$LD_LIBRARY_PATH" | column -s: -t)"
    LibraryPaths="$LibraryPaths:$LDLibraryPaths"
}

function resolveNativeDependency() {
    local readonly Dep="$1"

    if [[ -z "$LibraryPaths" ]]; then set_LibraryPaths; fi

    local readonly OLD_IFS="$IFS"
    IFS=':'
    for path in $LibraryPaths
    do
        local depPath=''
        depPath="$(readlink -f --no-newline "$path/$Dep")"
        if [[ "$?" -eq 0 && -r "$depPath" ]]
        then
            echo -n " $depPath"
            IFS="$OLD_IFS"
            return
        fi
    done
    IFS="$OLD_IFS"

    echo "Could not find '$Dep' in [$LibraryPaths]" 1>&2
    echo 'Consider adding the appropriate path to LD_LIBRARY_PATH.' 1>&2
    exit 42
}

function exitIfError() {
    local readonly ExitCode="$?"
    if [[ "$ExitCode" -ne 0 ]]; then exit "$ExitCode"; fi
}

function printHDFSInstructions() {
    local readonly HDFSPath='/lib/storm/1.0.1'
    cat <<HDFS_NOTICE
To deploy the to HDFS, do something like the following:

    hdfs fs -mkdir -p '$HDFSPath'
    hdfs fs -put '$ZipFileName' '$HDFSPath'

HDFS_NOTICE
}

function printUsage() {
    cat <<USAGE_STRING
$_ThisScript <ZIP_FILE>
Updates the given zip file (in-place) to add storms native dependency libraries.
USAGE_STRING

    echo
    printHDFSInstructions
    exit 1
}

function processArguments() {
    if [[ "$#" -ne 1 ]]
    then 
        printUsage
    fi

    if [[ ! -r "$1" || ! -w "$1" ]]
    then
        echo "Cannot access '$1'"
        exit 1
    fi

    if [[ ! -w "$(dirname "$0")" ]]
    then
        echo "Cannot write to the working directory"
        exit 1
    fi

    ZipFileName="$(readlink -f "$1")"
    if [[ ! -r "$ZipFileName" ]]
    then
        echo "File '$ZipFileName' is not readable."
        exit 1
    fi
}

function set_NativeDepsPaths() {
    for dep in $Dependencies;
    do
        local DepPath
        DepPath="$(resolveNativeDependency "$dep" "$LibraryPaths")"
        exitIfError
        NativeDepsPaths="$NativeDepsPaths $DepPath"
    done
}

# This is hard-coded from storm.
readonly Dependencies='libjzmq.so libzmq.so'


function createTarball() {
    processArguments "$@"
    set_NativeDepsPaths
    printHDFSInstructions
    echo "Adding native dependencies to '$ZipFileName':"
    zip -v -j -u "$ZipFileName" $NativeDepsPaths
    exitIfError
}

readonly _ThisScript="$(basename "$(readlink -f "$0")")" 

if [[ "$_ThisScript" == 'create-tarball.sh' ]]
then
    createTarball "$@"
fi

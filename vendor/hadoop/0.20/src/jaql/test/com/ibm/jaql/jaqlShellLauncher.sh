#!/bin/bash

# Copyright (C) IBM Corp. 2009.
# 
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

# Use $1 to set JAQL_HOME 
JAQL_HOME=$1
shift 1
cygwin=false
case "`uname`" in
    CYGWIN*) cygwin=true;;
esac
if $cygwin; then
  JAQL_HOME=`cygpath "$JAQL_HOME"`
fi
export JAQL_HOME=$JAQL_HOME

# To run Jaql shell with mimi cluster, Hadoop-related environment variables
# should be unset.
unset HADOOP_HOME
unset HADOOP_CONF_DIR
unset HBASE_HOME
unset HBASE_CONF_DIR

export DFLT_HADOOP_VERSION=0.20
export DFLT_HBASE_VERSION=0.20.0

"${JAQL_HOME}/bin/jaqlshell" "-c" "$@"

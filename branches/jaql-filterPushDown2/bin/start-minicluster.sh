#!/bin/sh
# 
# Start a hadoop minicluster

if [ "$JAQL_HOME" = "" ]; then
	echo "Error, JAQL_HOME must be set"
	exit 1
fi

if [ "$MINICLUSTER_HOME" = "" ]; then
	export MINICLUSTER_HOME=${JAQL_HOME}/minicluster
fi

export HADOOP_CONF_OVERRIDE_DIR="${MINICLUSTER_HOME}/conf"
export HADOOP_LOG_DIR="${MINICLUSTER_HOME}/logs"
outFile="${MINICLUSTER_HOME}/minicluster.out"
pidFile="${MINICLUSTER_HOME}/minicluster.pid"

rm -f ${HADOOP_LOG_DIR}/*

echo "starting minicluster..."
nohup ${JAQL_HOME}/bin/jaql com.ibm.jaql.MiniCluster "-Dhadoop.minicluster.dir=${MINICLUSTER_HOME}" "$@" > "$outFile" 2>&1 < /dev/null &
# ${JAQL_HOME}/bin/jaql com.ibm.jaql.MiniCluster "-Dhadoop.minicluster.dir=${MINICLUSTER_HOME}" "$@" 
pid=$!

echo $pid > $pidFile
echo "started minicluster under process $pid"

sleep 3
tail ${HADOOP_LOG_DIR}/minicluster.out

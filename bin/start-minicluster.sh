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

echo "starting minicluster in ${MINICLUSTER_HOME}"

export HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.minicluster.dir=${MINICLUSTER_HOME}"
export HADOOP_CONF_OVERRIDE_DIR="${MINICLUSTER_HOME}/conf"
export HADOOP_LOG_DIR="${MINICLUSTER_HOME}/logs"
outFile="${MINICLUSTER_HOME}/minicluster.out"
pidFile="${MINICLUSTER_HOME}/minicluster.pid"

rm -fr ${HADOOP_LOG_DIR}/history
rm -f ${HADOOP_LOG_DIR}/*

echo "launching minicluster..."
${JAQL_HOME}/bin/jaql com.ibm.jaql.MiniCluster "$@" > "$outFile" 2>&1 < /dev/null &
pid=$!

echo $pid > $pidFile
echo "started minicluster under process $pid"

sleep 3
tail ${outFile}

export HADOOP_CONF_DIR=${HADOOP_CONF_OVERRIDE_DIR}
export HADOOP_CONF_OVERRIDE_DIR=
export PS1="minicluster> "

echo "Running shell configured for the minicluster."
echo "Type exit to terminate minicluster."
$SHELL -i

. ${JAQL_HOME}/bin/stop-minicluster.sh



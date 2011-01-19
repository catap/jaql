#!/bin/sh
# 
# Start a hadoop minicluster

if [ "$JAQL_HOME" = "" ]; then
	echo "Error, JAQL_HOME must be set"
	exit 1
fi

if [ "$MINICLUSTER_HOME" = "" ]; then
	MINICLUSTER_HOME=${JAQL_HOME}/minicluster
fi

pidFile="${MINICLUSTER_HOME}/minicluster.pid"

if [ ! -f $pidFile ]; then
  echo "Error: $pidFile not found"
  exit 1
fi

pid=`cat $pidFile`

echo "stopping minicluster under process $pid"
kill $pid

rm $pidFile


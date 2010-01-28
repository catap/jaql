# Copyright (C) IBM Corp. 2008.
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

import os,sys,string,re
from os import listdir

#find out all the jars in path, return them in a list
def _getjarfilelist(path):
	filelist=[]#empty list
	for item in listdir(path):#iterate dir, find all jar files and add them to the list
		tems=string.split(item,".")
		filetype=tems[len(tems)-1]
		if filetype=="jar":
			filelist.append(os.path.join(path,item))
	return filelist

"""
Configure jaql environment, 0 or 1 parameter. By given one boolean parameter (optional), (True of False), True to print debug info, False do not.
"""
def _configenv(*args):
	_SEPARATO="" # system classpath separator
	_SEP=os.sep # system path separator
	_CLASSPATH = "" # JVM classpath parameters
	_DFLT_HADOOP_VERSION = "0.20.1" # default Hadoop version
	_HADOOP_HOME="" # HADOOP_HOME
	_JAQL_HOME="" # JAQL_HOME
	_PLATFORM="" #platform name

	if os.name=="nt":
		_SEPARATO=";"#windows
		_PLATFORM="windows"
	elif os.name=="posix":
		_SEPARATO=":"#linux
		_PLATFORM="linux"

	#==================== JAQL_HOME ====================
	if os.environ.has_key("JAQL_HOME"):
		#JAQL_HOME is required
		_JAQL_HOME=os.environ["JAQL_HOME"]
	else:
		#if JAQL_HOME is not set, force to exit
		exit("[PyJaql] ERROR:JAQL_HOME MUST BE SET")

	#==================== DFLT_HADOOP_VERSION ====================
	if os.environ.has_key("DFLT_HADOOP_VERSION"):
		_DFLT_HADOOP_VERSION = os.environ["DFLT_HADOOP_VERSION"] #by default is 0.18.3

	#==================== HADOOP_HOME ====================
	if os.environ.has_key("HADOOP_HOME"):
		#if HADOOP_HOME is set
		_HADOOP_HOME = os.environ["HADOOP_HOME"]
	else:# if missed HADOOP_HOME, search "{JAQL_HOME}/vendor/hadoop/${DFLT_HADOOP_VERSION}"
		_HADOOP_HOME = _JAQL_HOME + _SEP + "vendor" + _SEP + "hadoop" + _SEP + _DFLT_HADOOP_VERSION

	#====================HADOOP_CONF_DIR ENV VAR====================
	if os.environ.has_key("HADOOP_CONF_DIR"):
		#if HADOOP_CONF_DIR is set, luanch jaql in cluster model
		_HADOOP_CONF_DIR = os.environ["HADOOP_CONF_DIR"]
	else:	# local model
		_HADOOP_CONF_DIR = _HADOOP_HOME + _SEP + "conf"
		
	if(len(args)==1):
		print "PATH INFO-------------------------------------------"
		#PRINT DEBUG INFO
		print "| _PLATFORM = %s" % (_PLATFORM)
		#print "_SEPARATO = %s" % (_SEPARATO) 
		#print "_SEP = %s" % (_SEP) 
		print "| _DFLT_HADOOP_VERSION = %s" % (_DFLT_HADOOP_VERSION) 
		print "| _HADOOP_HOME = %s" % (_HADOOP_HOME) 
		print "| _JAQL_HOME = %s" % (_JAQL_HOME)
		print "| _HADOOP_CONF_DIR = %s" % (_HADOOP_CONF_DIR)
		print "----------------------------------------------------"

	#(1) add jaql.jar to classpath
	for jar in _getjarfilelist(_JAQL_HOME):
		if jar.find("jaql.jar")>0:
			_CLASSPATH = _CLASSPATH + _SEPARATO + jar

	for jar in _getjarfilelist(os.path.join(_JAQL_HOME,"build")):
		if jar.find("jaql.jar")>0:
			_CLASSPATH = _CLASSPATH + _SEPARATO + jar
	
	#(2) add JAQL_HOME_CONF to classpath
	_CLASSPATH = _CLASSPATH + _SEPARATO + os.path.join(_JAQL_HOME,"conf")	
	
	#(3) add hadoop jars to classpath
	for jar in _getjarfilelist(_HADOOP_HOME):
		if jar.find("hadoop-") >=0: # filter jars with "hadoop-"
			_CLASSPATH = _CLASSPATH + _SEPARATO + jar
	for jar in _getjarfilelist(os.path.join(_HADOOP_HOME,"lib")):
		_CLASSPATH = _CLASSPATH + _SEPARATO + jar

	#(4) add hadoop_conf_dir to classpath
	_CLASSPATH = _CLASSPATH + _SEPARATO + _HADOOP_CONF_DIR
	
	return _CLASSPATH

"""
Check if all the required the jars are incuded in path, if yes, return True, else return False.
@path
@debug: boolean value. True to print debug info, false not
"""
def _check_env(path,debug):
	_ENV_CONF=True
	#Required path includes:
	#--jaql.jar
 	#--hadoop-[version]-core.jar
	#--commons-logging-[version].jar
	#--commons-logging-api-[version].jar
	#--log4j-[version].jar
	if path.find("jaql.jar")<0:
		print "[ENV ERROR] jaql.jar not found. Is jaql.jar under $JAQL_HOME or $JAQL_HOME/build ?"
		_ENV_CONF=False
	else:
		if debug:print "jaql.jar ---- ok"
		else:pass

	hadoopjar=re.findall("hadoop-[^a-zA-Z]*-core.jar",path)
	if len(hadoopjar)==0:
		print "[ENV ERROR] hadoop-[version]-core.jar not found."
		_ENV_CONF=False
	else:
		if debug:print "%s ---- ok" % (hadoopjar[0])
		else:pass

	commonsloggingjar=re.findall("commons-logging-[^a-zA-Z]*.jar",path)
	if len(commonsloggingjar)==0:
		print "[ENV ERROR] commons-logging-[version].jar not found"
		_ENV_CONF=False
	else:
		if debug:print "%s ---- ok" % (commonsloggingjar[0])
		else:pass
	
	commonsloggingapijar=re.findall("commons-logging-api-[^a-zA-Z]*.jar",path)
	if len(commonsloggingapijar)==0:
		print "[ENV ERROR] commons-logging-api-[version].jar not found"
		_ENV_CONF=False
	else:
		if debug:print "%s ---- ok" % (commonsloggingapijar[0])
		else:pass

	log4jjar=re.findall("log4j-[^a-zA-Z]*.jar",path)
	if len(log4jjar)==0:
		print "[ENV ERROR] log4j-[version].jar not found"
		_ENV_CONF=False
	else:
		if debug:print "%s ---- ok" % (log4jjar[0]) 
		else:pass

	if debug:
		print "----------------------------------------------------"
		if _ENV_CONF:
			print "success"
		else:
			print "failed"
		print "----------------------------------------------------"

	return _ENV_CONF

if __name__== "__main__":
       	path=_configenv(True)
	_check_env(path,True)
	
	
			
	
	


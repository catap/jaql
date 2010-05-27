
## Copyright (C) IBM Corp. 2008.
##  
## Licensed under the Apache License, Version 2.0 (the "License"); you may not
## use this file except in compliance with the License. You may obtain a copy of
## the License at
##  
## http://www.apache.org/licenses/LICENSE-2.0
##  
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
## WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
## License for the specific language governing permissions and limitations under
## the License.

import jpype
import json,types
from env import _configenv,_check_env

#pyJaql Exceptions---
class JVMException(Exception):
	def __init__(self, msg):
		self.msg=msg

class JAQLRuntimeException(Exception):
	def __init__(self, msg):
		self.msg=msg

class JarLoadingException(Exception):
	def __init__(self, msg):
		self.msg=msg
#--------------------

class Jaql:

	""" Initialize JAQL engine, read init parameters from env.ini  """
	def __init__(self):
		try:	
			if jpype.isJVMStarted():
				#if JVM is running, share the running JVM
				pass
			else:   #if no JVM is running, start a JVM			
				path = _configenv()
				if _check_env(path,False):#check if all required jars are included in path
					pass
				else:
					exit("Failed")
				jpype.startJVM(jpype.getDefaultJVMPath(),"-Djava.class.path="+path)
		except Exception,err:
			#Know JVM limitation may fire this exception 
			print err
			raise JVMException("Non-specific error occurs when attemps to start a JVM")
		finally:
			#initialize jaql engine
			pkgcom=jpype.JPackage("com")
			Jaql=pkgcom.ibm.jaql.lang.Jaql
			self.jaql=Jaql()

	"""
	Execute a jaql expression and return a generator
	@Arguments: args is a list to take mutable arguments. 
	@ #1 jaql query expression  |  <str>  | [required]
        @ #2 jaql query variable set  | <dict> | [optional]
	@return : result set | generator
	"""
	def execute(self,*args):
		#1 jaqlExp = "expression only"
		#2 jaqlExp = "expression" % dict
		#3 jaqlExp = "expression" % {..dict data..}
		try:		
			assert len(args)<=2
		except Exception,err:
			print "Bad query expression : function execute takes 1 or 2 parameters, you input %s" % len(args)

		self.jaql.setInput(args[0])		

		if len(args)==1:		
			#expression only, do nothing, prepared to execute
			pass
		else:
			expr=args[0]
			params=args[1]
			#expression with parameter extension, setVars to the expression
			for key in iter(params):
				varkey=key.rjust(len(key)+1,'$')#adjust key format, comply with "$key" format
				self.__setVar(varkey,params[key])#set variables
		
		#execute jaql query
		try:
			it=self.jaql.iter()
			while it.moveNext():
				jv=it.current()
				#if type is JsonAtom, convert to python
				if isinstance(jv,jpype.JPackage("com").ibm.jaql.json.type.JsonAtom):
					jv_str=jv.toString()				
					if isinstance(jv,jpype.JPackage("com").ibm.jaql.json.type.JsonString):
						#conver JsonString to str
						#json_value=jv_str.center(len(str(jv_str))+2,'"')# add " surround the str, make it to be valid JSON format	print "****************** is jsonstring *************************"
						yield str(jv_str)
					else:
						#eval expr and return right format data				
						yield json.loads(str(jv_str))
					
				else: 	
					#if type is JsonArray or JsonRecord, use json module to convert them to python types						
					#JsonArray -> list
					#JsonRecord -> dict
					yield json.loads(str(it.current().toString()))
		except Exception,err:
			raise JAQLRuntimeException(str(err))

	"""
	Execute jaql expression which is defined in a file.
	@Arguments: args is a list to take mutable arguments. 
	@ #1 jaql query expression  |  <str>  | [required]
        @ #2 jaql script file path  | <str> | [optional]
	@return : result set | generator
	"""
	def execute_script(self,*args):
	
		try:		
			assert len(args)<=2
		except Exception,err:
			print "Bad query expression : function execute takes 1 or 2 parameters, you input %s" % len(args)
		# extract script file, prepared to execute	
		JFile=jpype.JPackage("java").io.File
		f=JFile(args[0])
		self.jaql.setInput(args[0],jpype.JPackage("java").io.FileReader(f))
		if len(args)==1:
			#do nothing
			pass
		else:
			expr=args[0]
			params=args[1]
			#expression with parameter extension, setVars to the expression
			for key in iter(params):
				varkey=key.rjust(len(key)+1,'$')#adjust key format, comply with "$key" format
				self.__setVar(varkey,params[key])#set variables
		
		#execute jaql query
		try:
			it=self.jaql.iter()
			while it.moveNext():
				jv=it.current()
				#if type is JsonAtom, convert to python
				if isinstance(jv,jpype.JPackage("com").ibm.jaql.json.type.JsonAtom):
					jv_str=jv.toString()
					if isinstance(jv,jpype.JPackage("com").ibm.jaql.json.type.JsonString):
						#conver JsonString to str
						#json_value=jv_str.center(len(str(jv_str))+2,'"')# add " surround the str, make it to be valid JSON format
						yield str(jv_str)
					else:
						#eval expr and return right format data					
						yield json.loads(str(jv_str))
				else: 	
					#if type is JsonArray or JsonRecord, use json module to convert them to python types						
					#JsonArray -> list
					#JsonRecord -> dict
					print it.current()
					yield json.loads(str(it.current().toString()))
		except Exception,err:
			raise JAQLRuntimeException(str(err))

	"""
	Evaluate jaql expression
	@Arguments: args is a list to take mutable arguments. 
	@ #1 jaql query expression  |  <str>  | [required]
        @ #2 jaql query variable set  | <dict> | [optional]
	@return : result set | <list>
	"""
	def evaluate(self,*args):
		try:		
			assert len(args)<=2
		except Exception,err:
			print "Bad query expression : function execute takes 1 or 2 parameters, you input %s" % len(args)
		self.jaql.setInput(args[0])		
		if len(args)==1:		
			#expression only, do nothing, prepared to execute
			pass
		else:
			expr=args[0]
			params=args[1]
			#expression with parameter extension, setVars to the expression
			for key in iter(params):
				varkey=key.rjust(len(key)+1,'$')#adjust key format, comply with "$key" format
				self.__setVar(varkey,params[key])#set variables
		try:
			value=self.jaql.evalNext()
			return json.loads(str(value.toString()))
		except Exception,err:
			raise JAQLRuntimeException(str(err))

	"""
	Evaluate jaql script
	@Arguments: args is a list to take mutable arguments. 
	@ #1 jaql query expression  |  <str>  | [required]
    @ #2 jaql query variable set  | <dict> | [optional]
	@return : result set | <list>
	"""
	def evaluate_script(self,*args):
		try:		
			assert len(args)<=2
		except Exception,err:
			print "Bad query expression : function execute takes 1 or 2 parameters, you input %s" % len(args)
		# extract script file, prepared to execute	
		JFile=jpype.JPackage("java").io.File
		f=JFile(args[0])
		self.jaql.setInput(args[0],jpype.JPackage("java").io.FileReader(f))
		if len(args)==1:
			#do nothing
			pass
		else:
			expr=args[0]
			params=args[1]
			#expression with parameter extension, setVars to the expression
			for key in iter(params):
				varkey=key.rjust(len(key)+1,'$')#adjust key format, comply with "$key" format
				self.__setVar(varkey,params[key])#set variables
		try:
			value=self.jaql.evalNext()
			return json.loads(str(value.toString()))
		except Exception,err:
			raise JAQLRuntimeException(str(err))
	

	"""
	private function
	Set values to variable, when use setVar() in jaql.java, we did type convertion to satisfy it's argument spec
		#|    python   |    jaql      |
		#|-------------|--------------|	
		#| StringType  |  JsonString  | 
		#| IntType     |  JsonDecimal | 
		#| FloatType   |  JsonDouble  |
		#| LongType    |  JsonLong    |
		#| BooleanType |  JsonBool    |
	"""
	def __setVar(self,varName,value):	
		#Refer to jaql package com.ibm.jaql.json.type		
		pkg=jpype.JPackage("com").ibm.jaql.json.type
		if type(value)==types.StringType:
			self.jaql.setVar(varName,pkg.JsonString(value))
		if type(value)==types.IntType:
			self.jaql.setVar(varName,pkg.JsonDecimal(value))
		if type(value)==types.FloatType:
			self.jaql.setVar(varName,pkg.JsonDouble(value))
		if type(value)==types.LongType:
			self.jaql.setVar(varName,pkg.JsonLong(value))
		if type(value)==types.BooleanType:	
			if value :
				self.jaql.setVar(varName,pkg.MutableJsonBool(True))	
			else:	
				self.jaql.setVar(varName,pkg.MutableJsonBool(False))
	
	"""
	Add extension jars, enable 3rd party user definded functions.
	@Argument: path - jar file path
	"""
	def add_jar(self,path):
		try:		
			self.jaql.addJar(path)
		except JarLoadingException,err:
			raise JarLoadingException("jar not found")

	"""
	Set property for jaql engine, jaql supports 3 runtime properties, they are "enableRewrite" (default true),"stopOnException" (default true) and "JaqlPrinter"
	@Arguments:
        @ #1 the name of the propery. VALID NAMES: "enableRewrite", "stopOnException", "JaqlPrinter"
	@ #2 the value of the property are str
	@    if property name is "enableRewrite" , value should be "true" or "false" 
	@    if property name is "stopOnException", value should be "true" or "false" 
        @    if property name is "JaqlPrinter", value should be valid printer class name
	"""
	def set_property(self,name,value):
		self.jaql.setProperty(name,value)		

	"""
	Get property value by name
	@Argument: property name
	@Return: property value <str>
	"""
	def get_property(self,name):
		return str(self.jaql.getProperty(name))

	def isJVMStarted(self):
		return jpype.isJVMStarted()
	"""
	Close JAQL engine
	"""
	def close(self):
		if jpype.isJVMStarted():
			jpype.shutdownJVM()

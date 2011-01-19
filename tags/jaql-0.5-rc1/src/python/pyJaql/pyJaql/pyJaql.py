# -*- coding: utf-8 -*-
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

class JaqlRuntimeException(Exception):
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
			raise JVMException("Non-specific error occurs when attempt to start a JVM")
		finally:
			#initialize jaql engine
			pkgcom=jpype.JPackage("com")
			Jaql=pkgcom.ibm.jaql.lang.JaqlQuery
			self.jaql=Jaql()
		
	"""
	Register a jaql function
	@argument
		fn : function declaration
	"""	
	def register_function(self,fn):
		self.jaql.setQueryString(fn)
	
	"""
	Evaluate a jaql function
	@argument
		fn : function declaration
	"""			
	def eval_function(self,*args):
		try:	
			assert len(args)<=2
		except Exception,err:
			raise JaqlRuntimeException("Bad query expression : function eval_function takes 1 or 2 parameters, you input %s" % len(args))
		FunctionArgs=jpype.JPackage("com").ibm.jaql.lang.FunctionArgs
		funArgs=FunctionArgs()
		try:
			if len(args)==1:		
				#given null arguments
				value=self.jaql.evaluate(args[0],funArgs)
				if value!=None:
					if isinstance(value,jpype.JPackage("com").ibm.jaql.json.type.JsonAtom):
						return self.__parseJsonValue(value)
					else:
						return json.loads(self.__parseJsonValue(value))
			else:
				#set args for jaql function
				if isinstance(args[1],types.ListType) or isinstance(args[1],types.TupleType):
					funArgs.setArguments(args[1])#in sequence
				if isinstance(args[1],types.DictionaryType):#by key-value
					for key in args[1]:
						funArgs.setArgument(key,args[1][key])
				value=self.jaql.evaluate(args[0],funArgs)
				if value!=None:
					if isinstance(value,jpype.JPackage("com").ibm.jaql.json.type.JsonAtom):
						return self.__parseJsonValue(value)
					else:
						return json.loads(self.__parseJsonValue(value))
		except Exception,err:
			raise JaqlRuntimeException(err)
		
	def exec_function(self,*args):
		try:	
			assert len(args)<=2
		except Exception,err:
			raise JaqlRuntimeException("Bad query expression : function eval_function takes 1 or 2 parameters, you input %s" % len(args))
		FunctionArgs=jpype.JPackage("com").ibm.jaql.lang.FunctionArgs
		funArgs=FunctionArgs()
		try:
			if len(args)==1:		
				#given null arguments
				it=self.jaql.iterate(args[0],funArgs)
				while it.moveNext():
					value=it.current()
					if isinstance(value,jpype.JPackage("com").ibm.jaql.json.type.JsonAtom):
						yield self.__parseJsonValue(value)
					else:
						yield json.loads(self.__parseJsonValue(value))
			else:
				#set args for jaql function
				if isinstance(args[1],types.ListType) or isinstance(args[1],types.TupleType):
					funArgs.setArguments(args[1])#in sequence
				if isinstance(args[1],types.DictionaryType):#by key-value
					for key in args[1]:
						funArgs.setArgument(key,args[1][key])
				it=self.jaql.iterate(args[0],funArgs)
				while it.moveNext():
					value=it.current()
					if isinstance(value,jpype.JPackage("com").ibm.jaql.json.type.JsonAtom):
						yield self.__parseJsonValue(value)
					else:
						yield json.loads(self.__parseJsonValue(value))
		except Exception,err:
			raise JaqlRuntimeException(err)		
	
	"""
	register a batch script, prepare to execute
	"""	
	def exectue_batch(self, *args):
		try:
			self.jaql.setQueryString(args[0])
			self.__setVars(args)
		except Exception,err:
			raise JaqlRuntimeException(str(err))
		
	"""
	move to next cursor, if it's evaluate-able statement, evaluate it and set the current result
	"""
	def move_next(self):
		return self.jaql.moveNextQuery()
	
	"""get current result"""
	def current(self):
		it=self.jaql.currentQuery()
		while it.moveNext():
			jv=it.current()
			if isinstance(jv,jpype.JPackage("com").ibm.jaql.json.type.JsonAtom):
				    #if type is JsonAtom, convert to python	types and return	
					yield self.__parseJsonValue(jv)
			else: 	
				#if type is JsonArray or JsonRecord, use json module to convert them to python types						
				#JsonArray -> list
				#JsonRecord -> dict
				yield json.loads(self.__parseJsonValue(jv))
	
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
		#execute jaql query
		try:
			self.jaql.setQueryString(args[0])
			self.__setVars(args)
			it=self.__iterate()
			for item in it:
				yield item
		except Exception,err:
			raise JaqlRuntimeException(str(err))


	"""
	Evaluate jaql expression
	@Arguments: args is a list to take mutable arguments. 
	@ #1 jaql query expression  |  <str>  | [required]
        @ #2 jaql query variable set  | <dict> | [optional]
	@return : result set | <list>
	"""
	def evaluate(self,*args):
		try:
			self.jaql.setQueryString(args[0])
			self.__setVars(args)
			return self.__evaluate()
		except Exception,err:
			raise JaqlRuntimeException(str(err))

	
	def execute_script(self,filepath):
		try:
			self.jaql.exectueBatch(filepath)
		except Exception,err:
			raise JaqlRuntimeException(err)	

	"""
	Add extension jars, enable 3rd party user definded functions.
	@Argument: path - jar file path
	"""
	def add_jar(self,path):
		try:		
			self.jaql.addJar(path)
		except Exception,err:
			raise JaqlRuntimeException("jar not found")
		
	"""
	Set module search path
	@Argument : 
		module path
	"""
	def set_module_path(self,*args):
		try:
			JString=jpype.JPackage("java").lang.String
			strs=JString(args[0])
			self.jaql.setModuleSeachPath(strs)
		except Exception,err:
			raise JaqlRuntimeException(err)
		
	"""
	register a java udf
	@Arguments
	@#1 udfname
		java udf name
	@#2 udfpath
		java udf class path
	"""
	def register_java_udf(self,udfname,udfpath):
		try:
			self.jaql.registerJavaUDF(udfname,udfpath)
		except Exception,err:
			raise JaqlRuntimeException(err)
	
	"""
	Evaluate a jaql query
	"""
	def __evaluate(self):
		value=self.jaql.evaluate()
		if value!=None:
			if isinstance(value,jpype.JPackage("com").ibm.jaql.json.type.JsonAtom):
				return self.__parseJsonValue(value)
			else:
				return json.loads(self.__parseJsonValue(value))
	
	"""
	Execute a jaql query
	"""
	def __iterate(self):
		it=self.jaql.iterate()
		while it.moveNext():
			jv=it.current()
			if isinstance(jv,jpype.JPackage("com").ibm.jaql.json.type.JsonAtom):
				yield self.__parseJsonValue(jv)
			else:
				yield json.loads(self.__parseJsonValue(jv))  
				
	"""
	Set arguments for jaql query
	"""
	def __setVars(self,args):
		try:	
			assert len(args)<=2
		except Exception,err:
			raise JaqlRuntimeException("Bad query expression : function execute takes 1 or 2 parameters, you input %s" % len(args))
		if len(args)==1:		
			#expression only, do nothing, prepared to execute
			pass
		else:
			if isinstance(args[1],types.DictionaryType): 
				params=args[1]
				#expression with parameter extension, setVars to the expression
				for key in iter(params):
					#varkey=key.rjust(len(key)+1,'$')#adjust key format, comply with "$key" format
					self.__setVar(key,params[key])#set variables 
			else:
			     raise JaqlRuntimeException("Argument type %s is not usable" % type(args[1]))
	
	"""
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
			if value : self.jaql.setVar(varName,pkg.MutableJsonBool(True))
			else : self.jaql.setVar(varName,pkg.MutableJsonBool(False))


	"""
	parse json value to native python types
	if is atom json value, parse to python basic type
	if is jsonarrya or jsonrecord, return its equivalent str value
	if is null, just return
	"""
	def __parseJsonValue(self, json):
		pkg=jpype.JPackage("com").ibm.jaql.json.type
		if isinstance(json,pkg.JsonString):
			return str(json.toString())
		if isinstance(json,pkg.JsonDecimal):
			return json.intValue()
		if isinstance(json,pkg.JsonDouble):
			return json.doubleValue()
		if isinstance(json,pkg.JsonLong):
			return json.longValue()
		if isinstance(json,pkg.JsonBool):
			if json.get()==1 : return True
			else : return False
		if isinstance(json,pkg.JsonRecord):
			return json.toString()
		if isinstance(json,pkg.JsonArray):
			return json.toString()
		else : return json
		
	"""
	return true if jvm is already started
	"""	
	def isJVMStarted(self):
		return jpype.isJVMStarted()
	
	"""
	Close JAQL engine
	"""
	def close(self):
		if jpype.isJVMStarted():
			jpype.shutdownJVM()
			

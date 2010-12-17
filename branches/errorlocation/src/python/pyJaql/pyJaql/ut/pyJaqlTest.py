# -*- coding: utf-8 -*-
import unittest,types,sys,os
from pyJaql import *


"""
Unit test cases of pyJaql
"""
class pyJaqlTestCase(unittest.TestCase):

	BASE_DIR = os.getenv("JAQL_HOME", "");
	
	def setUp(self):
		self.jaql=Jaql()

	def tearDown(self):
		pass 

	def testJVMRunningStatus(self):
		assert jpype.isJVMStarted() == 1 
	
	def testTypes(self):
		try:
			it=self.jaql.evaluate("[{'name':'alex','age':20,'isStudent':true},{'name':'jack','age':23,'isStudent':false}];");
			'''result set should be a json record'''
			expected_name=('alex','jack')
			expected_age=(20,23)
			expected_bool=(True,False)
			i=0
			for people in it:
				assert people['name']==expected_name[i]
				assert people['age']==expected_age[i]
				assert people['isStudent']==expected_bool[i]
				i=i+1
		except Exception,err:
			print err.msg
	
	'''test hdfs write and read'''	
	def testHdfsWriteAndRead(self):
		#write
		try:
			it1=self.jaql.execute("[{'writeSample':'data'}]->write(hdfs('testHdfsWrite'));")
			for metadata in it1:
				assert metadata["type"]=="hdfs" , 'Failed to write data to hdfs'
				assert metadata["location"]=="testHdfsWrite" , 'Failed to write data to hdfs'
		except JaqlRuntimeException,err:
			print err.msg		
		#read		
		it2=self.jaql.execute("read(hdfs('testHdfsWrite'));")
		for data in it2:
			assert data["writeSample"]=="data" , 'Failed to read data from hdfs'


	'''test jaql transform , dict result'''
	def testTransform(self):
		it=self.jaql.execute("[{'name':'alex','age':20,'weight':55.8,'isStudent':true},{'name':'jack','age':23,'weight':64.1,'isStudent':false}]->transform {$.name,$.age,$.weight,$.isStudent};")
		for dicts in it:
			assert isinstance(dicts["name"],basestring), 'Field is not StringType'
			assert type(dicts["age"])==types.IntType , 'Field is not IntType'
			assert type(dicts["isStudent"])==types.BooleanType , 'Field is not BooleanType'
			assert type(dicts["weight"])==types.FloatType , 'Field is not FloatType'

	'''test jaql filter'''
	def testFilter(self):
		it=self.jaql.execute("[{'name':'alex','age':20,'isStudent':true},{'name':'jack','age':23,'isStudent':false}]->filter $.isStudent==true->transform $.name;")
		for student in it:
			assert student=="alex" , "Failed with filter"

	'''test jaql use of short-hand projection notation'''
	def testShortHand(self):
		it=self.jaql.execute("[{'name':'alex','age':20,'isStudent':true},{'name':'jack','age':23,'isStudent':false}][*].name; ")
		except_name=['alex','jack']		
		i=0		
		for people in it:
			assert people==except_name[i] , 'Failed with short-hand projection notation'
			i=i+1

	'''test jaql expand'''
	def testExpand(self):
		it=self.jaql.execute("[{name:'Jon Doe', movie_ids:[3,65,8,72]}, {name:'Jane Dean', movie_ids:[5,98,2,65]}]-> expand $.movie_ids;")
		except_ids=[3,65,8,72,5,98,2,65]		
		i=0				
		for people in it:
			assert people==except_ids[i] , 'Failed with Expand'
			i=i+1
	
	'''test jaql join'''
	def testJoin(self):
		it=self.jaql.execute("$users = [{name: 'Jon Doe', password: 'asdf1234', id: 1},{name: 'Jane Doe', password: 'qwertyui', id: 2},{name: 'Max Mustermann', password: 'q1w2e3r4', id: 3}];$pages = [{userid: 1, url:'code.google.com/p/jaql/'},{userid: 2, url:'www.cnn.com'},{userid: 1, url:'java.sun.com/javase/6/docs/api/'}];join $users, $pages where $users.id == $pages.userid into {$users.name, $pages.*};")
		expected_name=('Jane Doe','Jon Doe','Jon Doe')
		i=0
		for user in it:
			assert type(user)==types.DictType , 'Failed with Join'
			assert user['name']==expected_name[i]
			i=i+1

	'''test jaql group'''
	def testGroup(self):
		it=self.jaql.execute("[{id:1, dept: 1, income:12000},{id:2, dept: 1, income:13000},{id:3, dept: 2, income:15000},{id:4, dept: 1, income:10000},{id:5, dept: 3, income:8000},{id:6, dept: 2, income:5000},{id:7, dept: 1, income:24000}] -> group by $dept_group = $.dept into {$dept_group, total: sum($[*].income)};")
		except_total=[20000,59000,8000]
		i=0		
		for item in it:
			assert item["total"]==except_total[i] , 'Failed with group'
			i=i+1

	'''test jaql function definition'''
	def testFunctionDefinition(self):
		it=self.jaql.execute("$introMessage = fn($input, $id) ($input -> filter $.from == $id-> transform { mandatory: $.msg }); [{ from: 101, to: [102],ts: 1243361567, msg: 'Hello, world!'},{ from: 201, to: [20, 81, 94],ts: 1243361567,msg: 'Hello, world! was interesting, but lets start a new topic please' },{ from: 81, to: [201, 94, 40],ts: 1243361567, msg: 'Agreed, this topic is not for Joe, but more suitable for Ann' },{ from: 40, to: [201, 81, 94],ts: 1243361567,msg: 'Thanks for including me on this topic about nothing... reminds me of a Seinfeld episode.'},{ from: 20, to: [81, 201, 94],ts: 1243361567, msg: 'Where did the topic go.. hopefully its more than about nothing.' }  ] -> $introMessage(101);")
		for item in it:
			assert item["mandatory"] == "Hello, world!" , 'Faild to define function $introMessage() '

	'''test setVar in jaql statement'''
	def testSetVar(self):
		try:
			it=self.jaql.execute("[{'name':'alex','age':20,'isStudent':true},{'name':'jack','age':23,'isStudent':false},{'name':'jack','age':25,'isStudent':false}]->filter $.name==$name and $.age==$age-> transform $.age;",{'$name':'jack','$age':23})
			for item in it:
				assert item==23,'Faild in setVar'
		except JaqlRuntimeException,e:
			print e.msg
		
	'''test evaluate'''
	def testEvaluate(self):
		v=self.jaql.evaluate("[{'name':'alex','age':20,'isStudent':true},{'name':'jack','age':23,'isStudent':false}]->filter $.isStudent==true->transform $.name;")
		expected_names=('alex','jack')
		i=0
		for name in v:
			assert name==expected_names[i]
			i=i+1
			
	def testMutipleQueries(self):
		try:
			value1=self.jaql.evaluate("[1,2,3][0];")
			value2=self.jaql.evaluate("[2,3,4][0];");
			assert value1==1
			assert value2==2
		except Exception,err:
			print err.msg
				
	'''method evaluate() does not support multiple statements, expect exception'''
	def testMutipleStatementExceptionWhenEval(self):
		try:
			self.jaql.evaluate("1+1;2+2;")
		except JaqlRuntimeException,e:
			assert e.msg=="java.lang.IllegalArgumentException: Illegal statements, multiple statements not allowed."
	
	'''method execute() does not support multiple statements, expect exception'''	
	def testMutipleStatementExceptionWhenExec(self):
		try:
			self.jaql.execute("1+1;2+2;")
		except JaqlRuntimeException,e:
			assert e.msg=="java.lang.IllegalArgumentException: Illegal statements, multiple statements not allowed."
	
	'''support import and use jaql module '''			
	def testJaqlModule(self):
		search_p=self.BASE_DIR+"/src/test/com/ibm/jaql/modules"
		try:
			self.jaql.set_module_path(search_p)
			value1=self.jaql.evaluate("import fuzzy_join;")
			value2=self.jaql.evaluate("fuzzy_join::cleanTitle('XY');");
			assert value2=="XY"
		except JaqlRuntimeException,e:
			print e.msg
			
	'''support register and use a java udf function'''
	def testRegisterFunction(self):
		ext_jar=self.BASE_DIR+"/build/extension.jar"
		try:
			self.jaql.add_jar(ext_jar)
			self.jaql.register_java_udf("split","com.acme.extensions.fn.Split1")
			value=self.jaql.evaluate("split('a,b,c,d,e',',');");
			assert value==['a','b','c','d','e']
		except Exception,err:
			print err.msg
			
	def testReuseStatement(self):
		try:
			v1=self.jaql.evaluate("[{name:'jack',age:20},{name:'mike',age:30}] -> filter $.name==$name -> transform $.age;",{"$name":"jack"});
			assert v1[0]==20
			v2=self.jaql.evaluate("[{name:'jack',age:20},{name:'mike',age:30}] -> filter $.name==$name -> transform $.age;",{"$name":"mike"});
			assert v2[0]==30
		except Exception,err:
			print err.msg
	
	'''define a jaql function and evaluate the function'''
	def testEvalFunction(self):
		try:
			self.jaql.register_function("sample=fn(itg,str,float,bool)([{id:itg, name:str, height:float,isStudent: bool}]->transform [$.id,$.name,$.height,$.isStudent]);")
			infos=self.jaql.eval_function("sample",[12345,'jack',66.3,True]);
			assert infos[0][0]==12345
			assert infos[0][1]=='jack'
			assert infos[0][2]==66.3
			assert infos[0][3]==True
		except Exception,err:
			print err.msg
		
	'''define a jaql function and execute the function'''	
	def testExecFunction(self):
		try:
			self.jaql.register_function("sample=fn(itg,str,float,bool)([{id:itg, name:str, height:float,isStudent: bool}]->transform [$.id,$.name,$.height,$.isStudent]);")
			infos=self.jaql.exec_function("sample",[12345,'jack',66.3,True]);
			for info in infos:
				assert info[0]==12345
				assert info[1]=='jack'
				assert info[2]==66.3
				assert info[3]==True
		except Exception,err:
			print err.msg			
	
	'''set sequence arguments for jaql function'''
	def testSetSequenceArgumentsForFunction(self):
		try:
			self.jaql.register_function("samplefn=fn(a,b,c,d,e)(a+b+c+d+e);")
			v=self.jaql.eval_function("samplefn",[1,2,3,4,5]);
			assert v==15
		except Exception,err:
			print err.msg
			
	'''set key-value arguments for jaql function'''
	def testSetIndividulArguments(self):
		try:
			self.jaql.register_function("samplefn=fn(a,b)(a+b);")
			v=self.jaql.eval_function("samplefn",{'a':1200,'b':3200});
			assert v==4400 
		except Exception,err:
			print err.msg
			
	'''null argument test'''	
	def testNullFnArgument(self):
		try:
			self.jaql.register_function("samplefn=fn()(3+4+5);")
			v=self.jaql.eval_function("samplefn");
			assert v==12
		except Exception,err:
			print err.msg
	
	'''exectue a bunch of scripts'''		
	def testExecuteBatchScript(self):
		self.jaql.exectue_batch("1+1;2+2;3+3;")
		exp_num=(2,4,6)
		i=0
		while self.jaql.move_next():
			it = self.jaql.current()
			for num in it:
				assert num==exp_num[i]
			i=i+1
		
if __name__ == "__main__":
	suite = unittest.TestLoader().loadTestsFromTestCase(pyJaqlTestCase)
	result=unittest.TextTestRunner(verbosity=2).run(suite)
	sys.stdout.write(str(result.wasSuccessful())+'\n')
	sys.stdout.write("Total number of test: %s \n" % (result.testsRun))
	sys.stdout.write("Errors: %d \n" % (len(result.errors)))
	sys.stdout.write("Failures: %d \n" % (len(result.failures)))
	if len(result.errors)!=0:
		sys.stdout.write("ERRORS------------------------------------------------------------------------------------------------: \n ")
		for error in result.errors:
			sys.stdout.write(str(error[0])+'\n')
			sys.stdout.write(str(error[1])+'\n')
	if len(result.failures)!=0:
		sys.stdout.write("FAILURES-----------------------------------------------------------------------------------------------: \n ")
		for failures in result.failures:
			sys.stdout.write(str(failures[0])+'\n')
			sys.stdout.write(str(failures[1])+'\n')



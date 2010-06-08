
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

import unittest,types,sys
from pyJaql import *

"""
Unit test cases of pyJaql
"""
class pyJaqlTestCase(unittest.TestCase):

	def setUp(self):
		self.jaql=Jaql()

	def tearDown(self):
		pass 

	def testJVMRunningStatus(self):
			assert isJVMStarted() == 1 , 'Failed to run JVM'

	'''test jaql transform , dict result'''
	def testTransform(self):
		it=self.jaql.execute("[{'name':'alex','age':20,'weight':55.8,'isStudent':true},{'name':'jack','age':23,'weight':64.1,'isStudent':false}]->transform {$.name,$.age,$.weight,$.isStudent};")
		for dicts in it:
			assert type(dicts["name"])==types.StringType , 'Field is not StringType'
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
		for item in it:
			assert item==except_name[i] , 'Failed with short-hand projection notation'
			i=i+1

	'''test jaql expand'''
	def testExpand(self):
		it=self.jaql.execute("[{name:'Jon Doe', movie_ids:[3,65,8,72]}, {name:'Jane Dean', movie_ids:[5,98,2,65]}]-> expand $.movie_ids;")
		except_ids=[3,65,8,72,5,98,2,65]		
		i=0				
		for item in it:
			assert item==except_ids[i] , 'Failed with Expand'
			i=i+1
	
	'''test jaql join'''
	def testJoin(self):
		it=self.jaql.execute("$users = [{name: 'Jon Doe', password: 'asdf1234', id: 1},{name: 'Jane Doe', password: 'qwertyui', id: 2},{name: 'Max Mustermann', password: 'q1w2e3r4', id: 3}];$pages = [{userid: 1, url:'code.google.com/p/jaql/'},{userid: 2, url:'www.cnn.com'},{userid: 1, url:'java.sun.com/javase/6/docs/api/'}];join $users, $pages where $users.id == $pages.userid into {$users.name, $pages.*};")
		for item in it:
			assert type(item)==types.DictType , 'Failed with Join'

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
		it=self.jaql.execute("[{'name':'alex','age':20,'isStudent':true},{'name':'jack','age':23,'isStudent':false}]->filter $.name==$name and $.age==$age->transform $.name;",{'name':'alxe','age':20})
		for alex in it:
			assert alex=='alxe' , 'Failed with setVar'

	'''test evaluate'''
	def testEvaluate(self):
		value=self.jaql.evaluate("[{'a':'a1','b':'b1'},{'a':'a2','b':'b2'}] -> transform $.a;")	
		assert value[0]=='a1','evaluate failed'

	'''test set_property and get_property'''
	def testSetAndGetPropery(self):
		self.jaql.set_property("enableRewrite","true")
		self.jaql.set_property("stopOnException","true")
		assert self.jaql.get_property("enableRewrite")=="true"
		assert self.jaql.get_property("stopOnException")=="true"	
		
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
	


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

from distutils.core import setup
import os, os.path, sys

class pyJaqlSetup:
	#def setupWindows(self)
	#def setupLinux(self)
	#def setupMac(self)
	def setup(self):
		setup(
			name = 'pyJaql',
			package_dir={'pyJaql': 'pyJaql','pyJaql.ut':'pyJaql/ut'},
			#install packages		    	
			packages = ['pyJaql','pyJaql.ut'],
			#configurate file			
			package_data={'pyJaql': ['ut/*.jql']},
		    	version = '0.1',
		    	description = 'jaql - python - bridge',
		    	author='Wei Wei Yang-Jaql-IBM',
		    	author_email='impliance-jaql@cs.opensource.ibm.com'
		)

module = pyJaqlSetup()
module.setup()



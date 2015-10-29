/*
 * Copyright (C) IBM Corp. 2008.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.ibm.jaql;

import junit.framework.TestCase;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class TestPyJaql extends TestCase {

	/**
	 * Run pyJaql test script- "pyJaqlTest.py" 
	 * 		(path :src/test/com/ibm/jaql/pyJaqlTest.py) 
	 * Test result will be recorded in "testPyJaql.log" 
	 * 		(path : "build/test/logs/testPyJaql.log")
	 * 
	 * Before Run this test, required:
	 * Python , jpype and pyJaql are correctly installed
	 * 
	 */
	public void testPyJaql() {

		Process proc = null;
		Runtime runtime = Runtime.getRuntime();
		List<String> list = new Vector<String>();
		Map<String, String> sysenv = System.getenv();// get system environment
		// variables
		Iterator<String> it = sysenv.keySet().iterator();
		while (it.hasNext()) {
			String key = it.next();
			list.add(key + "=" + sysenv.get(key));// add system environment
			// variables to JVM runtime
			// variables
		}
		list.add("PYTHONPATH=src/python/pyJaql ");
		String[] env = new String[list.size()];
		System.arraycopy(list.toArray(), 0, env, 0, list.size());
		try {
			proc = runtime.exec("python src/test/com/ibm/jaql/pyJaqlTest.py",
					env);// execute test script
		} catch (IOException e) {
			e.printStackTrace();
		}
		InputStream stdin = proc.getInputStream();
		InputStreamReader isr = new InputStreamReader(stdin);
		BufferedReader br = new BufferedReader(isr);
		String line = null;
		String result = null;
		try {
			// record the test result in "testPyJaql.log"
			BufferedWriter recorder = new BufferedWriter(new FileWriter(
					"build/test/run.pyJaqlTest"));
			int lineNum = 0;
			while ((line = br.readLine()) != null) {
				lineNum++;
				if (lineNum == 1) {
					result = line;
				}
				recorder.write(line + "\n");
			}
			recorder.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// Check the result, expect "True"
		// if execution of pyJaqlTest.py is passed, result will be "True"
		// if there are any errors or failures when executing the script, result will be "False"
		assertEquals("True", result);

	}

}

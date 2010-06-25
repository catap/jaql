/*
 * Copyright (C) IBM Corp. 2009.
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
package com.ibm.jaql.io;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;

public class TestEncodings {

		@Test
		public void jsonUTF8() throws Exception {
			
			String filename = System.getProperty("test.cache.data") + File.separator + "data" + File.separator + "utf8test.txt";
			
			// read content from utf-8 encoded file
			File file = new File( filename );
			BufferedReader rdr = new BufferedReader( new InputStreamReader( new FileInputStream( file ), "UTF-8"));
			String line = rdr.readLine();
			
			// create json data structure that includes content
			BufferedJsonRecord rec = new BufferedJsonRecord();
			rec.add(new JsonString("utf8"), new JsonString(line));
			rec.add(new JsonString("utf8Bytes"), new JsonString(line.getBytes("UTF-8")));
			
			// get the string from json
			String s1 = JsonUtil.printToString(rec);
			String s2 = rec.toString();
			
			// compare to original content
			JsonRecord test = (JsonRecord)(new JsonParser()).parse(s1);
			assertEquals("UTF-8 encoding problem from JsonUtil", rec, test);
			
			test = (JsonRecord)(new JsonParser()).parse(s2);
			assertEquals("UTF-8 encoding problem from toString", rec, test);
		}
		
		@Test
		public void jsonUTF8ToString() throws Exception {
			String filename = System.getProperty("test.cache.data") + File.separator + "data" + File.separator + "utf8test.txt";
			String[] in = getInputData(filename);
			JsonValue[] jsonIn = generateComplexJsonValues(in);
			String[] out = toStringRoundTrip(jsonIn);
			JsonValue[] jsonOut = parseJsonValues(out);
			for(int i = 0; i < jsonIn.length; i++) {
				assertEquals(jsonIn[i], jsonOut[i]);
			}
		}
		
		@Test
		public void jsonUTF8PrintToString() throws Exception {
			String filename = System.getProperty("test.cache.data") + File.separator + "data" + File.separator + "utf8test.txt";
			String[] in = getInputData(filename);
			JsonValue[] jsonComplex = generateComplexJsonValues(in);
			String[] outComplex = printToStringRoundTrip(jsonComplex);
			JsonValue[] jsonOut = parseJsonValues(outComplex);
			for(int i = 0; i < jsonComplex.length; i++) {
				assertEquals(jsonComplex[i], jsonOut[i]);
			}
			
			JsonValue[] jsonAtomic = generateAtomicJsonValues(in);
			String[] outAtomic = printToStringRoundTrip(jsonAtomic);
			JsonValue[] jsonOutAtomic = parseJsonValues(outAtomic);
			for(int i = 0; i < jsonAtomic.length; i++) {
				assertEquals(jsonAtomic[i], jsonOutAtomic[i]);
			}
		}
		
		@Test
		public void jsonUTF8ToFile() throws Exception {
			
		}
		
		@Test
		public void jsonUTF8ToHadoop() throws Exception {
			
		}
		
		private String[] getInputData(String fname) throws Exception {
			ArrayList<String> list = new ArrayList<String>();
			
			// read content from utf-8 encoded file
			File file = new File( fname );
			BufferedReader rdr = new BufferedReader( new InputStreamReader( new FileInputStream( file ), "UTF-8"));
			String line = null;
			while( (line = rdr.readLine()) != null) {
				list.add(line);
			}
			
			String[] arr = new String[list.size()];
			return list.toArray(arr);
		}
		
		private JsonValue[] generateAtomicJsonValues(String[] s) throws Exception {
			// for each string, test the following json values:
			// 0. JsonString from the input string
			// 1. JsonString from the bytes of the input string
			int NUM_VAL_TESTS = 2;
			int numVals = s.length * NUM_VAL_TESTS;
			JsonValue[] vals = new JsonValue[numVals];
			for(int i = 0; i < s.length; i++) {
				
				for(int j = 0; j < NUM_VAL_TESTS; j++) {
					JsonValue v = null;
					switch(j) {
					case 0:
						v = new JsonString(s[i]);
						break;
					case 1:
						v = new JsonString(s[i].getBytes("UTF-8"));
						break;
					default:
						break;
					}
					vals[i*NUM_VAL_TESTS + j] = v;
				}
			}
			return vals;
		}
		
		private JsonValue[] generateComplexJsonValues(String[] s) throws Exception {
			
			// for each string, test the following json values:
			// 0. JsonRecord with JsonString and JsonString from bytes
			// 1. JsonRecord with nested JsonRecord as above
			// 2. JsonArray with JsonString and JsonString from bytes
			// 3. JsonArray with nested JsonArray as above
			// 4. JsonRecord with nested JsonRecord and JsonArray
			// 5. JsonArray with nested JsonRecord and JsonArray
			int NUM_VAL_TESTS = 6;
			int numVals = s.length * NUM_VAL_TESTS;
			JsonValue[] vals = new JsonValue[numVals];
			for(int i = 0; i < s.length; i++) {
				JsonString js = new JsonString(s[i]);
				JsonString jsBytes = new JsonString(s[i].getBytes("UTF-8"));
				BufferedJsonRecord rec = new BufferedJsonRecord();
				rec.add(new JsonString("s"), js);
				rec.add(new JsonString("sBytes"), jsBytes);
				BufferedJsonArray arr = new BufferedJsonArray();
				arr.add(js);
				arr.add(jsBytes);
				for(int j = 0; j < NUM_VAL_TESTS; j++) {
					JsonValue v = null;
					switch(j) {
					case 0:
						v = rec;
						break;
					case 1:
						BufferedJsonRecord r = new BufferedJsonRecord();
						r.add(new JsonString("nested"), rec);
						v = r;
						break;
					case 2:
						v = arr;
						break;
					case 3:
						BufferedJsonArray a = new BufferedJsonArray();
						a.add(arr);
						v = a;
						break;
					case 4:
						BufferedJsonRecord r2 = new BufferedJsonRecord();
						r2.add(new JsonString("nestedRec"), rec);
						r2.add(new JsonString("nestedArr"), arr);
						v = r2;
						break;
					case 5:
						BufferedJsonArray a2 = new BufferedJsonArray();
						a2.add(rec);
						a2.add(arr);
						v = a2;
						break;
					default:
						break;
					}
					vals[i*NUM_VAL_TESTS + j] = v;
				}
			}
			return vals;
		}
		
		private JsonValue[] parseJsonValues(String[] s) throws Exception {
			JsonParser p = new JsonParser();
			JsonValue[] vals = new JsonValue[s.length];
			for(int i = 0; i < vals.length; i++) {
				vals[i] = p.parse(s[i]);
			}
			return vals;
		}
		
		private String[] toStringRoundTrip(JsonValue[] vals) throws Exception {
			String[] sArr = new String[vals.length];
			
			for(int i = 0; i < vals.length; i++) {
				sArr[i] = vals[i].toString();
			}
			return sArr;
		}
		
		private String[] printToStringRoundTrip(JsonValue[] vals) throws Exception {
			String[] sArr = new String[vals.length];
			
			for(int i = 0; i < vals.length; i++) {
				sArr[i] = JsonUtil.printToString(vals[i]);
			}
			return sArr;
		}
		
		private String[] toFileRoundTrip(JsonValue[] vals) throws Exception {
			return null;
		}
		
		private String[] toHadoopRoundTrip(JsonValue[] vals) throws Exception {
			return null;
		}
}
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Ignore;
import org.junit.Test;

import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.FunctionArgs;
import com.ibm.jaql.lang.JaqlQuery;


public class TestJaqlQuery extends TestCase{


	private static String publishers = "[{name: 'Scholastic', country: 'USA'}, "
			+ "{name: 'Grosset', country: 'UK'}, "
			+ "{name: 'Writers Publishing House', country: 'China'}]";
	
	private static String employees = "[" +
			"{id:1, dept: 1, income:12000}," +
			"{id:2, dept: 1, income:13000}," +
			"{id:3, dept: 2, income:15000}," +
			"{id:4, dept: 1, income:10000}," +
			"{id:5, dept: 3, income:8000}," +
			"{id:6, dept: 2, income:5000}," +
			"{id:7, dept: 1, income:24000}" +
			"]";
	
	private static final String BASE_DIR = System.getProperty("user.dir");
	private static final String DATADIR = BASE_DIR.replace('\\', '/') + "/build/src/test/com/ibm/jaql/data/";
	
	/**
	 * Write data to hdfs and read it back
	 */
	@Test
	public void testHDFSWriteAndRead(){
		try{
			JaqlQuery q = new JaqlQuery(employees + "->write(hdfs('" + DATADIR+ "emps'));");
			//q.setQueryString();
			q.evaluate();
			q.setQueryString("read(hdfs('"+DATADIR+"emps'))->filter $.income==10000->transform $.id;");
			JsonArray v = (JsonArray) q.evaluate();
			assertEquals(new JsonLong(4),  v.get(0) );
			q.close();
		}catch(Exception ex){
			ex.printStackTrace();
			fail(ex.getMessage());
		}
	}
	
	/**
	 * The most typical usage of JaqlQuery, set input for a query then evaluate, returns a json iterator
	 */
	@Test
	public void testIterate() {
		try{
			JaqlQuery q = new JaqlQuery();
			q.setQueryString(publishers + " -> filter $.country=='UK';");
			JsonIterator it = q.iterate();
			while(it.moveNext()){
				String p = ((JsonRecord)it.current()).get(new JsonString("name")).toString();
				assertEquals("Grosset", p);
			}
			q.close();
		}catch(Exception ex){
			fail(ex.getMessage());
		}
	}
	
	/**
	 * The most typical usage of JaqlQuery, set input for a query then evaluate, returns a json value
	 */
	@Test
	public void testEvaluate() {
		try{
			JaqlQuery q = new JaqlQuery();
			q.setQueryString(publishers + " -> filter $.country=='UK';");
			JsonValue v = q.evaluate();
			List<JsonValue> list = ((JsonArray)v).asList();
			for(JsonValue t:list) {
				String p = ((JsonRecord)t).get(new JsonString("name")).toString();
				assertEquals("Grosset", p);
			}
			q.close();
		}catch(Exception ex){
			fail(ex.getMessage());
		}
	}
	
	/**
	 * Use one JaqlQuery, set query string twice and evaluate.
	 */
	@Test
	public void testSetVar() {
		try{
			JaqlQuery q = new JaqlQuery();
			q.setQueryString(publishers + " -> filter $.country=='UK';");
			JsonValue v1 = q.evaluate();
			q.setQueryString(publishers + " -> filter $.country==$c;");
			q.setVar("$c", "UK");
			JsonValue v2 = q.evaluate();
			assertEquals(v1, v2);
			q.close();
		}catch(Exception ex){
			fail(ex.getMessage());
		}
	}
	

	/**
	 * Get a var's value from a context.
	 */
	@Test
	public void testGetVar() {
		try{
			JaqlQuery q = new JaqlQuery();
			q.setQueryString("(" + publishers + " -> filter $.country==$c)[0].country;");
			q.setVar("$c", "UK");
			assertEquals(new JsonString("UK"), q.getVar("$c"));
			q.close();
		}catch(Exception ex){
			fail(ex.getMessage());
		}
	}

	/**
	 * Group query
	 */
	@Test
	public void testGroup(){
		try{
			JaqlQuery q = new JaqlQuery();
			q.setQueryString("read(hdfs('"+DATADIR+"emps')) -> group by $dept_group = $.dept into {$dept_group, total: sum($[*].income)};");
			JsonIterator it = q.iterate();
			int[] expected = {59000,20000,8000};
			int i = 0;
			while(it.moveNext()){
				JsonRecord r = (JsonRecord) it.current();
				assertEquals(new JsonLong(expected[i]), (JsonLong)r.get(new JsonString("total")));
				i++;
			}
			q.close();
		}catch(Exception ex){
			fail(ex.getMessage());
		}
	}

	/**
	 * API function exception handling sample
	 */
	@Test
	public void testException(){
		try{
			JaqlQuery q = new JaqlQuery();
			q.setQueryString("aa;");
			q.evaluate();
			q.close();
		}catch(Exception ex){
			assertEquals("variable not defined: aa", ex.getMessage());
		}
	}
	
	/**
	 * User may have a bunch of udfs, addJar provides a way to import them in jaql engine.
	 * Use addJar to recognize a certain udf jar, the use registerFunction to register a udf in current context
	 * 
	 */
	@Test
	public void testRegisterFunction(){
		try{
			JaqlQuery q = new JaqlQuery();
			String path = BASE_DIR + "/build/extension.jar";
			File p = new File (path);
			if(!p.isFile()){
				return; 
			}
			q.addJar( path);
			q.registerJavaUDF("split","com.acme.extensions.fn.Split1");
			q.setQueryString("split('a,b,c,d,e',',');");
			JsonArray v = (JsonArray)q.evaluate();
			assertEquals(new JsonString("a"), v.get(0));
			q.close();
		}catch(Exception ex){
			fail(ex.getMessage());
		}
	}

	/**
	 * Set module search paths for Jaql engine and use modules
	 */
	//FIXME ----- THIS TEST CASE SHOULD BE FIXED SINCE THE ABSOLUTLY FILE PATH IS NOT RECOMMANDED
	@Test
	public void testImportModule(){
		try{
			JaqlQuery q = new JaqlQuery();
			String path = BASE_DIR+"/src/test/com/ibm/jaql/modules";
			File p = new File (path);
			if(!p.isDirectory()){
				return;
			}
			q.setModuleSeachPath(path);
			q.setQueryString("import fuzzy_join;");
			q.evaluate();
			q.setQueryString("fuzzy_join::cleanTitle('XY');");
			JsonValue v = q.evaluate();
			assertEquals("XY", v.toString());
			q.close();
		}catch(Exception ex){
			fail(ex.getMessage());
		}
	}

	
	/**
	 * Set json record var for a query, uses a BufferedJsonRecord instance
	 */
	@Test
	public void testSetRecordVar(){
		try{
			JaqlQuery q = new JaqlQuery();
			q.setQueryString("[{name: 'Scholastic', country: 'USA'},{name: 'Grosset', country: 'UK'},$publisher] -> expand [$.name];");			
			//$publisher = {name:'test',country:'China'}
			BufferedJsonRecord r = new BufferedJsonRecord();
			r.set(new JsonString("name"),new JsonString("test"));
			r.set(new JsonString("country"),new JsonString("China"));
			q.setVar("$publisher", r);
			//evaluate
			JsonValue v = q.evaluate();
			//the result is a JsonArray
			JsonArray ja = (JsonArray)v;
			JsonValue fv = ja.get(ja.count()-1);
			assertEquals("test",fv.toString());
			q.close();
		}catch(Exception ex){
			fail(ex.getMessage());
		}
	}

	/**
	 * Set json array var for a query,  uses a BufferedJsonArray instance
	 */
	@Test
	public void testSetArrayVar(){
		try{
			JaqlQuery q = new JaqlQuery();
			q.setQueryString(" [ $groubAteams ] -> transform $[0];");			
			BufferedJsonArray r = new BufferedJsonArray();
			r.add(new JsonString("Italy"));
			r.add(new JsonString("Paraguay"));
			r.add(new JsonString("New Zealand"));
			r.add(new JsonString("Slovakia"));
			q.setVar("$groubAteams", r);
			JsonArray v = (JsonArray)q.evaluate();
			assertEquals("Italy", v.get(0).toString());
			q.close();
		}catch(Exception ex){
			ex.printStackTrace();
			fail(ex.getMessage());
		}
	}
	
	/**
	 * Construct a nested json array and record variable, then pass this value to JaqlQuery
	 */
	@Test
	public void testSetNestedVar(){
		try{
			JaqlQuery q = new JaqlQuery();
			q.setQueryString("[{Group: 'A', team : ['South Africa','Mexico','Uruguay','France']}," +
					"{Group:'B', team : ['Argentina','Nigeria','Korea Republic','Greece']}," +
					"{Group:'C', team : ['England','USA','Algeria','Slovenia']}," +
					"{Group:'D', team : ['Germany','Australia','Serbia','Ghana']}," +
					"{Group:'E', team : ['Netherlands','Denmark','Japan','Cameroon']}," +
					"$groupF" +
					"] -> filter $.Group=='F' -> transform $.team -> transform $[0];");	
			//{Group:'F', team : ['Italy','Paraguay','New Zealand','Slovakia']}
			BufferedJsonRecord br = new BufferedJsonRecord();
			BufferedJsonArray teams = new BufferedJsonArray();
			teams.add(new JsonString("Italy"));
			teams.add(new JsonString("Paraguay"));
			teams.add(new JsonString("New Zealand"));
			teams.add(new JsonString("Slovakia"));
			br.add(new JsonString("Group"), new JsonString("F"));
			br.add(new JsonString("team"), teams);
			q.setVar("$groupF", br);
			JsonArray v = (JsonArray) q.evaluate();
			assertEquals("Italy", v.get(0).toString());
			q.close();
		}catch(Exception ex){
			fail(ex.getMessage());
		}
	}
	
	/**
	 * User may want to set a string value which denotes a json record/array to a variable
	 * when pass a string value to setVar(), if it's a valid json record/array, it can be directly
	 * set to a variable.
	 * If not valid, throws a exception. (refer to next test case)
	 */
	@Test
	public void testSetArray(){
		try{
			String gb = "['Argentina','Nigeria','Korea Republic','Greece']";
			JaqlQuery q = new JaqlQuery();
			q.setQueryString("[{Group: 'A', team : ['South Africa','Mexico','Uruguay','France']},{Group: 'B' , team : $groubB}] -> filter $.Group == 'B' -> transform $.team[0];");
			q.setArray("$groubB", gb);
			JsonArray v = (JsonArray)q.evaluate();
			assertEquals("Argentina", v.get(0).toString());
			q.close();
		}catch(Exception ex){
			fail(ex.getMessage());
			
		}
	}
	
	/**
	 * User passes a String variable, which represents a json record to jaql query, 
	 * jaql engine parse the string to json record if valid, then execute the query.
	 */
	@Test
	public void testSetRecord(){
		try{
			String gb = "{Group: 'B', team : ['Argentina','Nigeria','Korea Republic','Greece']}";
			JaqlQuery q = new JaqlQuery();
			q.setQueryString("[{Group: 'A', team : ['South Africa','Mexico','Uruguay','France']},$groubB] -> filter $.Group == 'B' -> transform $.team[0];");
			q.setRecord("$groubB", gb);
			JsonArray v = (JsonArray)q.evaluate();
			assertEquals("Argentina", v.get(0).toString());
			q.close();
		}catch(Exception ex){
			fail(ex.getMessage());
			
		}
	}
	/**
	 * If the string value is not a valid json value, nor a atom string, it will raise a exception
	 * Below gb obviously is a invalid json array, set this value to a variable will cause a "unexpected token: ::" 
	 */
	@Test
	public void testSetVarException(){
		try{
			String gb = "['Argentina','Nigeria':'Korea Republic','Greece']";
			JaqlQuery q = new JaqlQuery();
			q.setQueryString("[{Group: 'A', team : ['South Africa','Mexico','Uruguay','France']},{Group: 'B' , team : $groubB}] -> filter $.Group == 'B' -> transform $.team[0];");
			q.setArray("$groubB", gb);
			JsonArray v = (JsonArray)q.evaluate();
			assertEquals("Argentina", v.get(0).toString());
			q.close();
		}catch(Exception ex){
			assertEquals("com.ibm.jaql.json.parser.ParseException", ex.getClass().getName());	
		}
	}
	
	/**
	 * User declares a JaqlQuery statements, one may want to reuse this statement several times.
	 * Each time, one sets different values for the variables then evaluate to get result 
	 */
	@Test
	public void testReuseStatement(){
		try{
			JaqlQuery q = new JaqlQuery();
			q.setQueryString("[{name:'jack',age:20},{name:'mike',age:30}] -> filter $.name==$name -> transform $.age;");
			q.setVar("$name", "jack");
			JsonArray v1 = (JsonArray)q.evaluate();
			assertEquals(new JsonLong(20), (JsonLong)v1.get(0));
			q.setVar("$name", "mike");
			JsonArray v2 = (JsonArray)q.evaluate();
			assertEquals(new JsonLong(30), (JsonLong)v2.get(0));
			q.close();
		}catch(Exception ex){
			fail(ex.getMessage());
		}
	}
	
	/**
	 * User defines a jaql query, set parameters and evaluate via a loop
	 */
	@Test
	public void testLoop(){
		try{
			JaqlQuery q = new JaqlQuery();
			q.setQueryString("[{NO:1,size:10},{NO:2,size:20},{NO:3,size:30},{NO:4,size:40},{NO:5,size:50},{NO:6,size:60}]" +
					"-> filter $.NO == $NO -> transform $.size;");
			int totalSize = 0;
			for(int i=1;i<6;i++){
				q.setVar("$NO", i);
				JsonArray v = (JsonArray) q.evaluate();
				totalSize +=  Integer.parseInt(v.get(0).toString());
			}
			assertEquals(150, totalSize);
			q.close();
		}catch(Exception ex){
			fail(ex.getMessage());
		}
	}

	///////////////////////////////////////////////////
	///// FUNCTION RELATED
	///////////////////////////////////////////////////

	/**
	 * set arguments in sequence for jaql function
	 */
	@Test
	public void testSetSequenceArgumentsForFunction() {
		try {
			JaqlQuery jaql = new JaqlQuery("samplefn=fn(a,b,c,d,e)(a+b+c+d+e);");
			FunctionArgs args = new FunctionArgs();
			args.setArguments(1, 2, 3, 4, 5);
			JsonValue v = jaql.evaluate("samplefn", args);
			assertEquals(new JsonLong(15), (JsonLong) v);
			jaql.close();
		} catch (Exception ex) {
			fail(ex.getMessage());
		}

	}

	/**
	 * set arguments by name for jaql function
	 */
	@Test
	public void testSetIndividulArguments() {
		try {
			JaqlQuery jaql = new JaqlQuery("samplefn=fn(a,b)(a+b);");
			FunctionArgs args = new FunctionArgs();
			args.setArgument("a", 1200);
			args.setArgument("b", 1200);
			JsonValue v = jaql.evaluate("samplefn", args);
			assertEquals(new JsonLong(2400), (JsonLong) v);
			jaql.close();
		} catch (Exception ex) {
			fail(ex.getMessage());
		}
	}

	/**
	 * iterator a jaql function
	 */
	@Test
	public void testFunctionIterate() {
		try {
			JaqlQuery jaql = new JaqlQuery(
					"samplefn=fn()([{a:'a'},{a:'b'},{a:'c'}]);");
			FunctionArgs args = new FunctionArgs();
			JsonIterator it = jaql.iterate("samplefn", args);
			JsonString latest = new JsonString();
			while (it.moveNext()) {
				JsonValue v = it.current();
				JsonRecord rec = (JsonRecord) v;
				latest = (JsonString) rec.get(new JsonString("a"));
			}
			assertEquals(new JsonString("c"), latest);
			jaql.close();
		} catch (Exception ex) {
			ex.printStackTrace();
			fail(ex.getMessage());
		}
	}

	/**
	 * Set null argument for jaql function
	 */
	@Test
	public void testNullFnArgument() {
		try {
			JaqlQuery jq = new JaqlQuery();
			jq.setQueryString("[1,2,3,4,5]->write(hdfs('"+DATADIR+"foo'));");
			jq.evaluate();
			jq.setQueryString("sample=fn()(read(hdfs('"+DATADIR+"foo'))->transform $+1);");
			JsonArray array = (JsonArray)jq.evaluate("sample", null);
			int[] expected = {2,3,4,5,6};
			int i = 0;
			for (JsonValue x : array){
				assertEquals(new JsonLong(expected[i]), (JsonLong) x);
				i++;
			}
			jq.close();
		} catch (Exception ex) {
			fail(ex.getMessage());
		}
	}

	/**
	 * Set null argument object test for jaql function
	 */
	@Test
	public void testNullObject() {
		try {
			JaqlQuery jq = new JaqlQuery();
			jq.setQueryString("[1,2,3,4,5]->write(hdfs('"+DATADIR+"foo'));");
			jq.evaluate();
			jq.setQueryString("sample=fn()(read(hdfs('"+DATADIR+"foo'))->transform $+1);");
			JsonArray array = (JsonArray)jq.evaluate("sample", new FunctionArgs());
			int[] expected = {2,3,4,5,6};
			int i = 0;
			for (JsonValue x : array){
				assertEquals(new JsonLong(expected[i]), (JsonLong) x);
				i++;
			}
			jq.close();
		} catch (Exception ex) {
			fail(ex.getMessage());
		}
	}

	/**
	 * Set all types of arguments
	 */
	@Test
	public void testFnTypeArgument() {
		try {
			JaqlQuery jq = new JaqlQuery();
			jq.setQueryString("sample=fn(itg,str,double,bool,long,float)([{id:itg, name:str, weight:double, height:float , student: bool, online: long }]->transform $.name);");
			FunctionArgs args = new FunctionArgs();
			args.setArgument("itg", 12345);
			args.setArgument("str", "jack");
			args.setArgument("double", 66.3);
			args.setArgument("float", 171.5f);
			args.setArgument("bool", true);
			args.setArgument("long", 509874);
			JsonValue v = jq.evaluate("sample", args);
			JsonArray arr = (JsonArray) v;
			assertEquals(new JsonString("jack"), arr.get(0));
			jq.close();
		} catch (Exception ex) {
			fail(ex.getMessage());
		}
	}
	
	
	
	/**
	 * when given a query including multiple statements, use an iterator manner to execute the query.
	 */
	@Test
	public void testExecuteBatchScript(){
		try{
			JaqlQuery q = new JaqlQuery();
			q.setQueryString("1+1;2+2;3+3;");
			int[] expected = {2,4,6};
			int i = 0;
			while(q.moveNextQuery()){
				JsonIterator it = q.currentQuery(); 
				while(it.moveNext()){
					JsonValue v = it.current(); // 2 - 4 - 6
					assertEquals(new JsonLong(expected[i]), (JsonLong)v); 
				}
				i++;
			}
			q.close();
		}catch(Exception ex){
			fail(ex.getMessage());
		}
	}
	
	/**
	 * Multiple statements is not allowed when using evaluate()
	 */
	@Test
	public void testMultipleStatementsExceptionWhenEval(){
		try{
			JaqlQuery q = new JaqlQuery();
			q.setQueryString("1+1;1+2;");
			q.evaluate();
			q.close();
		}catch(Exception ex){
			assertEquals(IllegalArgumentException.class, ex.getClass());
			assertEquals("Illegal statements, multiple statements not allowed.", ex.getMessage());
		}
	}
	
	/**
	 * Multiple statements is not allowed when using iterate()
	 */
	@Test
	public void testMultipleStatementsExceptionWhenIter(){
		try{
			JaqlQuery q = new JaqlQuery();
			q.setQueryString("1+1;1+2;");
			q.iterate();
			q.close();
		}catch(Exception ex){
			assertEquals(IllegalArgumentException.class, ex.getClass());
			assertEquals("Illegal statements, multiple statements not allowed.", ex.getMessage());
		}
	}

}

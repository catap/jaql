package com.ibm.jaql;

import java.util.List;

import junit.framework.TestCase;

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.JaqlQuery;

public class TestJaqlQuery extends TestCase {

	private static String publishers = "[{name: 'Scholastic', country: 'USA'}, "
			+ "{name: 'Grosset', country: 'UK'}, "
			+ "{name: 'Writers Publishing House', country: 'China'}]";
	
	public void testIterate() {
		try{
			JaqlQuery q = new JaqlQuery();
			q.setQueryString(publishers + " -> filter $.country=='UK';");
			JsonIterator it = q.iterate();
			while(it.moveNext()){
				String p = ((JsonRecord)it.current()).get(new JsonString("name")).toString();
				assertEquals("Grosset", p);
			}
		}catch(Exception ex){
			fail("runs into error");
		}
	}
	
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
		}catch(Exception ex){
			fail("runs into error");
		}
	}
	
	public void testSetVar() {
		try{
			JaqlQuery q = new JaqlQuery();
			q.setQueryString(publishers + " -> filter $.country=='UK';");
			JsonValue v1 = q.evaluate();
			q.setQueryString(publishers + " -> filter $.country==$c;");
			q.setVar("$c", "UK");
			JsonValue v2 = q.evaluate();
			assertEquals(v1, v2);
		}catch(Exception ex){
			fail("runs into error");
		}
	}
	
	public void testGetVar() {
		try{
			JaqlQuery q = new JaqlQuery();
			q.setQueryString(publishers + " -> filter $.country==$c;");
			q.setVar("$c", "UK");
			JsonValue v = q.evaluate();
			assertEquals(new JsonString("UK"), v);
		}catch(Exception ex){
			fail("runs into error");
		}
	}

}

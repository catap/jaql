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
package com.ibm.jaql.lang;

import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;

/**
 * This is for Java developers to call JAQL in their Java applications.
 * Typical usage as follows:<br><br>
 * 
 * <code>
 * JaqlQuery q = new JaqlQuery();<br>
 * q.setQueryString("read(hdfs('books')) -> filter $.price < $maxprice");<br>
 * q.setVar("$maxprice", 100);<br>
 * JsonIterator it = q.iterate();
 * </code>
 * */
public class JaqlQuery {
	
	private CoreJaql jq;
	
	public JaqlQuery() {
		jq = new Jaql();
	}
	
	/**
	 * Sets the jaql query string to process.
	 * 
	 * @param string
	 * */
	public void setQueryString(String string) {
		jq.setInput(string);
	}
	
	/**
	 * Executes the jaql query, and returns the result in
	 * JsonIterator format.
	 * 
	 * @return JsonIterator
	 * */
	public JsonIterator iterate() throws Exception {
		return jq.iter();
	}
	
	/**
	 * Executes the jaql query, and returns the result in
	 * JsonValue format.
	 * 
	 * @return JsonValue
	 * */
	public JsonValue evaluate() throws Exception {
		return jq.eval();
	}
	
	/**
	 * Sets the designated variable to the given Java String value.
	 * 
	 * @param varName the variable name, for example, $name
	 * @param x the variable value
	 * */
	public void setVar(String varName, String x) {
		jq.setVar(varName, new JsonString(x));
	}
	
	/**
	 * Sets the designated variable to the given Java int value.
	 * 
	 * @param varName the variable name, for example, $year
	 * @param x the variable value
	 * */
	public void setVar(String varName, int x) {
		jq.setVar(varName, new JsonLong(x));
	}
	
	/**
	 * Sets the designated variable to the given Java long value.
	 * 
	 * @param varName the variable name, for example, $docid
	 * @param x the variable value
	 * */
	public void setVar(String varName, long x) {
		jq.setVar(varName, new JsonLong(x));
	}
	
	/**
	 * Sets the designated variable to the given Java float value.
	 * 
	 * @param varName the variable name, for example, $price
	 * @param x the variable value
	 * */
	public void setVar(String varName, float x) {
		jq.setVar(varName, new JsonDouble(x));
	}
	
	/**
	 * Sets the designated variable to the given Java double value.
	 * 
	 * @param varName the variable name, for example, $price
	 * @param x the variable value
	 * */
	public void setVar(String varName, double x) {
		jq.setVar(varName, new JsonDouble(x));
	}
	
	/**
	 * Sets the designated variable to the given Java boolean value.
	 * 
	 * @param varName the variable name, for example, $visible
	 * @param x the variable value
	 * */
	public void setVar(String varName, boolean x) {
		jq.setVar(varName, JsonBool.make(x));
	}
	
	/**
	 * Gets the value of the designated variable.
	 * 
	 * @param varName the variable name, for example, $firstname
	 * @return JsonValue
	 * */
	public JsonValue getVar(String varName) throws Exception {
		return jq.getVarValue(varName);
	}

}

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

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.expr.core.Expr;

public interface CoreJaql {

	/**
	 * Sets the query to process.
	 * 
	 * @param string
	 *            jaql query string
	 * */
	void setInput(String string);

	/**
	 * Sets the designated global variable to the given value.
	 * 
	 * @param varName
	 *            the variable name
	 * @param value
	 *            the variable value
	 * */
	void setVar(String varName, JsonValue value);

	/**
	 * Sets the designated global variable to the given value.
	 * 
	 * @param varName
	 * @param iter
	 */
	void setVar(String varName, JsonIterator iter);

	/**
	 * Gets the value of the designated global variable.
	 * 
	 * @param varName
	 * @return JsonValue
	 */
	JsonValue getVarValue(String varName) throws Exception;

	/**
	 * Gets the value of the designated global variable.
	 * 
	 * @param varName
	 * @return JsonIterator
	 */
	JsonIterator getVarIter(String varName) throws Exception;

	/**
	 * Materialize a variable if it hasn't been materialized yet.
	 * 
	 * @param name
	 *            The variable name
	 */
	void materializeVar(String name) throws Exception;

	/**
	 * Explains the statement
	 * 
	 * @return String
	 */
	String explain() throws Exception;

	/**
	 * Prepares the statement
	 * 
	 * @return
	 * @throws Exception
	 */
	Expr expr() throws Exception;

	/**
	 * Evaluates the next query in the script and returns an iterator over the
	 * result. If the result is not an array or null, it is coerced into an
	 * array. If there is no such query, return null.
	 * 
	 * @return JsonIterator
	 * @throws Exception
	 */
	JsonIterator iter() throws Exception;

	/**
	 * Evaluates the next query in the script and returns the result. If there
	 * is no such query, return null (which is ambiguous with a query that
	 * returns null).
	 * 
	 * @return JsonValue
	 * @throws Exception
	 */
	JsonValue eval() throws Exception;

	/**
	 * Sets the properties. Currently the property name can be enableRewrite,
	 * stopOnException, or JaqlPrinter.
	 * 
	 * @param name
	 *            The property name
	 * @param value
	 *            The property value
	 */
	void setProperty(String name, String value);

	/**
	 * Gets the value of the designated property.
	 * 
	 * @param name
	 * @return String
	 */
	String getProperty(String name);

	/**
	 * Adds an additional jar into classpath.
	 * 
	 * @param path
	 *            The jar file's path
	 */
	void addJar(String path) throws Exception;
	
	/**
	 * 
	 * Close a jaql query
	 * Jaql query statement should be closed after all the query is done, else it will cause a shutdown in progress exception
	 *
	 */
	void close();

}
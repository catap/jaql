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

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import antlr.RecognitionException;
import antlr.TokenStreamException;

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefineJaqlFunctionExpr;
import com.ibm.jaql.lang.parser.JaqlLexer;

public interface CoreJaql extends Closeable {

	/**
	 * Sets the query to process.
	 * 
	 * @param string
	 *            jaql query string
	 * */
	void setInput(String string);

	/**
	 * whether stops when exception occurs
	 * 
	 * @param bool
	 *            true / false
	 */
	void stopOnException(boolean bool);

	/**
	 * Set exception handler for jaql engine
	 * 
	 * @param exceptionHandler
	 *            exception handler class, extends ExceptionHandler
	 */
	void setExceptionHandler(ExceptionHandler exceptionHandler);

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
	 *            the variable name
	 * @param iter
	 *            the variable value
	 */
	void setVar(String varName, JsonIterator iter);

	/**
	 * Gets the value of the designated global variable.
	 * 
	 * @param varName
	 *            the variable name
	 * @return JsonValue the variable value
	 */
	JsonValue getVarValue(String varName) throws Exception;

	/**
	 * Gets the value of the designated global variable.
	 * 
	 * @param varName
	 *            the variable name
	 * @return JsonIterator the variable name
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
	 * @return String explain result
	 */
	String explain() throws Exception;

	/**
	 * Prepares the statement
	 * 
	 * @return prepared statement
	 * @throws Exception
	 */
	Expr expr() throws Exception;

	/**
	 * Evaluates the next query in the script and returns an iterator over the
	 * result. If the result is not an array or null, it is coerced into an
	 * array. If there is no such query, return null.
	 * 
	 * @return JsonIterator query result
	 * @throws Exception
	 */
	JsonIterator iter() throws Exception;

	/**
	 * Evaluates the next query in the script and returns the result. If there
	 * is no such query, return null (which is ambiguous with a query that
	 * returns null).
	 * 
	 * @return JsonValue query result
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
	 *            The property name
	 * @return String The property value
	 * 
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
	 * Invoke a jaql function by name with the given arguments, return the
	 * JsonValue result
	 * 
	 * @param fnName
	 *            jaql function's name
	 * @param args
	 *            jaql function arguments
	 * @return json iterator result
	 * @throws Exception
	 * 
	 */
	JsonIterator iterate(String fnName, FunctionArgs args) throws Exception;

	/**
	 * Invoke a jaql function by name with the given arguments, return the
	 * JsonValue result
	 * 
	 * @param fnName
	 *            jaql function's name
	 * @param args
	 *            jaql function arguments
	 * @return json value result
	 * @throws Exception
	 * 
	 */
	JsonValue evaluate(String fnName, FunctionArgs args) throws Exception;

	/**
	 * 
	 * Register a java udf function to jaql's context
	 * 
	 * @param udfName
	 *            udf name, which will be reused in jaql query, e.g "split"
	 * @param udfPath
	 *            udf path specifies udf class path, e.g
	 *            com.acme.extensions.fn.Split1
	 * @throws Exception
	 */
	void registerJavaUDF(String udfName, String udfPath) throws Exception;

	/**
	 * 
	 * Close a jaql query Jaql query statement should be closed after all the
	 * query is done, else it will cause a shutdown in progress exception
	 * 
	 * @throws IOException
	 * 
	 */
	void close() throws IOException;

	/**
	 * Prepares the next evaluate-able statement, if there isn't any, return false
	 * else return true. 
	 * And set current value to the evaluation result of current statement.
	 * 
	 * @return 
	 * 		true | false
	 */
	boolean moveNextQuery() throws Exception;

	/**
	 * return the evaluation result of current statement
	 * @return
	 * 		evaluation result of current statement
	 */
	JsonIterator currentQuery();

	/**
	 * evaluate given statement and return a json value, if multiple sentence is given, throw a illegal statement exception
	 */
	JsonValue evaluate() throws Exception ;

	/**
	 * evaluate given statement and return a json iterator, if multiple sentence is given, throw a illegal statement exception
	 * 
	 */
	JsonIterator iterate() throws Exception;

	/**
	 * Execute batch script file
	 * @param batchFile
	 *             batch file path
	 * @throws FileNotFoundException
	 */
	void executeBatch(String batchFile) throws FileNotFoundException;

}
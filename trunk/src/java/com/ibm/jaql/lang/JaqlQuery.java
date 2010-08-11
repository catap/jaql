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

import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.parser.ParseException;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Module;

/**
 * This is for Java developers to call JAQL in their Java applications. Typical
 * usage as follows:<br>
 * <br>
 * 
 * <code>
 * JaqlQuery q = new JaqlQuery();<br>
 * q.setQueryString("read(hdfs('books')) -> filter $.price < $maxprice");<br>
 * q.setVar("$maxprice", 100);<br>
 * JsonIterator it = q.iterate();
 * q.close();
 * </code>
 * */
public class JaqlQuery implements Closeable {

    private CoreJaql jq;
    private String query;

    List<JsonIterator> result = null;

    public JaqlQuery() {
        jq = new Jaql();
        jq.setExceptionHandler(new NullExceptionHandler());
        jq.stopOnException(true);
    }

    public JaqlQuery(String query) {
        jq = new Jaql();
        jq.setExceptionHandler(new NullExceptionHandler());
        jq.stopOnException(true);
        setQueryString(query);
    }

    /**
     * Sets the jaql query string to process.
     * 
     * @param string
     * */
    public void setQueryString(String query) {
        this.query = query;
        jq.setInput(query);
    }

    /**
     * Executes the jaql query, and returns the result in JsonIterator format.
     * 
     * @return JsonIterator
     * */
    public JsonIterator iterate() throws Exception {

        JsonIterator it = jq.iterate();
        setQueryString(query);
        return it;
    }

    /**
     * Executes the jaql query, and returns the result in JsonValue format.
     * 
     * @return JsonValue
     * */
    public JsonValue evaluate() throws Exception {

        JsonValue v = jq.evaluate();
        // set query again to rewind the statement
        setQueryString(query);
        return v;

    }

    /**
     * Sets the designated variable to the given Java String value.
     * 
     * @param varName
     *            the variable name, for example, $name
     * @param x
     *            the variable value
     * @throws Exception
     * */
    public void setVar(String varName, String x) throws Exception {
        setVar(varName, new JsonString(x));
    }

    /**
     * Sets the designated variable to the given Java int value.
     * 
     * @param varName
     *            the variable name, for example, $year
     * @param x
     *            the variable value
     * */
    public void setVar(String varName, int x) {
        jq.setVar(varName, new JsonLong(x));
    }

    /**
     * Sets the designated variable to the given Java long value.
     * 
     * @param varName
     *            the variable name, for example, $docid
     * @param x
     *            the variable value
     * */
    public void setVar(String varName, long x) {
        jq.setVar(varName, new JsonLong(x));
    }

    /**
     * Sets the designated variable to the given Java float value.
     * 
     * @param varName
     *            the variable name, for example, $price
     * @param x
     *            the variable value
     * */
    public void setVar(String varName, float x) {
        jq.setVar(varName, new JsonDouble(x));
    }

    /**
     * Sets the designated variable to the given Java double value.
     * 
     * @param varName
     *            the variable name, for example, $price
     * @param x
     *            the variable value
     * */
    public void setVar(String varName, double x) {
        jq.setVar(varName, new JsonDouble(x));
    }

    /**
     * Sets the designated variable to the given Java boolean value.
     * 
     * @param varName
     *            the variable name, for example, $visible
     * @param x
     *            the variable value
     * */
    public void setVar(String varName, boolean x) {
        jq.setVar(varName, JsonBool.make(x));
    }

    /**
     * Sets the designated variable to the given Json value.
     * 
     * @param varName
     *            the variable name
     * @param x
     *            the variable value
     * */
    public void setVar(String varName, JsonValue v) {
        if (v == null) {
            throw new IllegalArgumentException("Null argument");
        }
        jq.setVar(varName, v);
    }

    /**
     * Sets the designated variable to the given Java String value which is
     * array format, this value will be compiled to a JsonArray
     * 
     * @param varName
     *            the variable name, for example, $array
     * @param array
     *            the variable value, a String value
     * @throws Exception
     */
    public void setArray(String varName, String array) throws Exception {
        setVar(varName, parseJsonValueFromString(array));
    }

    /**
     * Sets the designated variable to the given Java String value which is
     * record format, this value will be compiled to a JsonRecord
     * 
     * @param varName
     *            the variable name, for example, $record
     * @param array
     *            the variable value, a String value
     * @throws Exception
     */
    public void setRecord(String varName, String record) throws Exception {
        setVar(varName, parseJsonValueFromString(record));
    }

    /**
     * Gets the value of the designated variable.
     * 
     * @param varName
     *            the variable name, for example, $firstname
     * @return JsonValue
     * */
    public JsonValue getVar(String varName) throws Exception {
        return jq.getVarValue(varName);
    }

    /**
     * Add the path of a jar to class loader
     * 
     * @param path
     *            the jar path
     * @throws Exception
     */
    public void addJar(String path) throws Exception {
        jq.addJar(path);
    }

    /**
     * Set Jaql Module search path, Jaql will search the given path for Jaql
     * modules
     * 
     * @param module
     *            module path
     * 
     */
    public void setModuleSeachPath(String[] path) {
        Module.setSearchPath(path);
    }

    /**
     * Set Jaql Module search path, Jaql will search the given path for Jaql
     * modules
     * 
     * @param module
     *            module path
     * 
     */
    public void setModuleSeachPath(String path) {
        Module.setSearchPath(new String[] { path });
    }

    /**
     * Parse a string value to JsonValue (JsonRecrod or JsonArray)
     * 
     * @param String
     *            value given string value
     * @return JsonValue corresponding json value
     * @throws Exception
     */
    protected static JsonValue parseJsonValueFromString(String value)
            throws ParseException {
        JsonParser parser = new JsonParser();
        JsonValue v = parser.parse(value);
        return v;
    }

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
    public void registerJavaUDF(String udfName, String udfPath)
            throws Exception {
        jq.registerJavaUDF(udfName, udfPath);
    }

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
    public JsonValue evaluate(String fnName, FunctionArgs args)
            throws Exception {

        return jq.evaluate(fnName, args);

    }

    /**
     * Invoke a jaql function by name with the given arguments, return the
     * JsonIterator result
     * 
     * @param fnName
     *            jaql function's name
     * @param args
     *            jaql function arguments
     * @return json iterator result
     * @throws Exception
     * 
     */
    public JsonIterator iterate(String fnName, FunctionArgs args)
            throws Exception {

        return jq.iterate(fnName, args);

    }

    /**
     * Close a jaql query Jaql query statement should be closed after all the
     * query is done.
     * 
     * @throws IOException
     * 
     */
    public void close() throws IOException {
        jq.close();
    }

    /**
     * Prepares the next evaluate-able statement, if there isn't any, return
     * false else return true. And set current value to the evaluation result of
     * current statement.
     * 
     * @return true | false
     */
    public boolean moveNextQuery() throws Exception {
        return jq.moveNextQuery();
    }

    /**
     * Evaluate current statement and return the result(JsonIterator)
     * 
     * @return
     */
    public JsonIterator currentQuery() {
        return jq.currentQuery();
    }

    /**
     * Execute a jaql script file
     * 
     * @param batchFile
     *            batch file path
     * @throws FileNotFoundException
     */
    public void exectueBatch(String batchFile) throws FileNotFoundException {
        jq.executeBatch(batchFile);
    }

}

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
package com.ibm.jaql.lang.expr.core;

import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.ExceptionHandler;
import com.ibm.jaql.lang.ThresholdExceptionHandler;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * @jaqlDescription Register a default exception handling policy.
 * Usage:
 * 
 * bool registerExceptionHandler( { errThresh: long } );
 * 
 * This function allows the default exception handling policy to be overridden.
 * Currently, the policy can specify how many exceptions to skip before propagating
 * the exception up the call stack. This is specified by the errThresh field of the
 * input. By default, errThresh is set to 0, meaning that no exceptions are skipped.
 * 
 * When an exception is skipped, the enclosing expression decides what to do. If the
 * exception occurs in the catch function, then it returns null and logs the results of
 * a user supplied expression. If the exception occurs in a transform, then the result is
 * skipped and logged.
 * 
 * @jaqlExample registerExceptionHandler({errThresh: 5});
 * 
 * @jaqlExample data = [ ["a",0], ["a",1], ["b",0], ["c",0], ["c",1], ["c",2]];
 * 
 * @jaqlExample data -> write(hdfs("test"));
 * 	 
 * @jaqlExample read(hdfs("test")) -> filter $[1] == 0 -> transform $.badTypeAssumption;
 * []
 */
public class RegisterExceptionHandler extends Expr {

	public static final String		ERROR_THRESH_NAME = "errThresh";
	public static final JsonString 	ERROR_THRESH_FIELD_NAME = new JsonString(ERROR_THRESH_NAME);		
	
	public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
	{
		public Descriptor()
		{
			super("registerExceptionHandler", RegisterExceptionHandler.class);
		}
	}
	
	public RegisterExceptionHandler(Expr[] exprs) {
		super(exprs);
	}
	
	@Override
	public JsonValue eval(Context context) throws Exception {
		JsonRecord opts = (JsonRecord)exprs[0].eval(context);
		JsonNumber tval = (JsonNumber)opts.get(ERROR_THRESH_FIELD_NAME);
		if(tval != null) {
			ExceptionHandler handler = new ThresholdExceptionHandler(tval.intValue());
			JaqlUtil.setExceptionHandler(handler);
		}
		return JsonBool.TRUE;
	}
	
	public static void readConf(String name, JobConf conf) throws Exception {
		String s = conf.get(name);
		JsonParser p = new JsonParser();
		JsonRecord opts = (JsonRecord)p.parse(s);
		JsonNumber tval = (JsonNumber)opts.get(ERROR_THRESH_FIELD_NAME);
		if(tval != null) {
			ExceptionHandler handler = new ThresholdExceptionHandler(tval.intValue());
			JaqlUtil.setExceptionHandler(handler);
		}
	}
	
	public static void writeConf(String name, JobConf conf) throws Exception {
		ThresholdExceptionHandler handler = (ThresholdExceptionHandler)JaqlUtil.getExceptionHandler();
		BufferedJsonRecord r = new BufferedJsonRecord();
		r.add(ERROR_THRESH_FIELD_NAME, new JsonLong(handler.getMaxExceptions()));
		String s = JsonUtil.printToString(r);
		conf.set(name, s);
	}
}
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
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
package com.ibm.jaql.lang.expr.core;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.EnumMap;
import java.util.Map;

import com.ibm.jaql.json.schema.OrSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.ExceptionHandler;
import com.ibm.jaql.lang.ThresholdExceptionHandler;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * Wrap this expression around any child expression that needs to be guarded
 * for exceptions. Usage is as follows:
 * 
 * catch(child expr, { errThresh: long } opts);
 * 
 * Note the following consequences of this expression:
 * 1. It is blocking: all values produced by child must be valid, 
 *                    which precludes streaming computation to the caller of "catch"
 * 2. If an error occurs in the child, the exception handling options determine what happens next.
 *    The errThresh lets the user specify how many errors can happen at the call-site of the specific
 *    catch expression. If the number of exceptions exceeds errThresh, then the exception is
 *    propagated up. Otherwise, the exception is simply logged. By default, if opts is not specified,
 *    errThresh is set to 0, e.g., exceptions propagate up. 
 */
public class CatchExpr extends Expr {
	
	private boolean configured = false;
	private ExceptionHandler handler;

	public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par12
	{
		public Descriptor()
		{
			super("catch", CatchExpr.class);
		}
	}

	public CatchExpr(Expr[] exprs)
	{
		super(exprs);
	}

	@Override
	public JsonValue eval(Context context) throws Exception {
		JsonValue v = null;
		if(!configured) {
			if(exprs[1] != null && exprs[1].getSchema().is(JsonType.RECORD).bool()) {
				JsonRecord opts = (JsonRecord)exprs[1].eval(context);
				JsonNumber tval = (JsonNumber)opts.get(RegisterExceptionHandler.ERROR_THRESH_FIELD_NAME);
				if(tval != null) handler = new ThresholdExceptionHandler(tval.intValue());
			} else {
				handler = JaqlUtil.getExceptionHandler();
			}
			configured = true;
		}
		
		try {
			v = exprs[0].eval(context);
		} catch(Throwable e) {
			handler.handleException(e);
		}
		return v;
	}

//	@Override
//	public Bool3 evaluatesChildOnce(int i) {
//		return Bool3.TRUE;
//	}

//	@Override
//	public Map<ExprProperty, Boolean> getProperties() {
//		Map<ExprProperty, Boolean> result = new EnumMap<ExprProperty, Boolean>(ExprProperty.class);
//	    // ALLOW_COMPILE_TIME_COMPUTATION not set; we want to make this explicit where desired
//		result.put(ExprProperty.ALLOW_COMPILE_TIME_COMPUTATION, false);
//	    result.put(ExprProperty.HAS_SIDE_EFFECTS, false);
//	    result.put(ExprProperty.IS_NONDETERMINISTIC, true);
//	    //result.put(ExprProperty.READS_EXTERNAL_DATA, false);
//	    //result.put(ExprProperty.HAS_CAPTURES, false);
//	    return result;
//	}

	@Override
	public Schema getSchema() {
		Schema s = exprs[0].getSchema();
		try {
			if(!s.matches(null)) {
				s = OrSchema.make(s, SchemaFactory.nullSchema());
			}
		} catch(Exception e) {
			// not sure what should be done here
			throw new UndeclaredThrowableException(e, "error when checking schema match");
		}
		return s;
	}

}
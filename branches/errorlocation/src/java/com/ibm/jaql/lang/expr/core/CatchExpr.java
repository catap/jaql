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
 * @jaqlDescription Wrap any expression with catch to guard against exceptions.
 * Usage:
 * 
 * T1|null catch( T1 e1, { errThresh: long } | null, T2 e2);
 * 
 * Wrap the catch expression around the first argument, e1, that needs to be guarded
 * for exceptions. 
 * 
 * The second argument is optional. It specifies an exception handling policy. If unspecified or null, the default
 * exception handling policy is used. By default, if an exception occurs, it is propagated
 * (which typically results in aborted execution). This default can be overridden globally
 * using the registerExceptionHandler function, or at can be overridden per usage of catch
 * by using the second argument. Such an override allows catch to throw an exception errThresh
 * times before propagating the exception. Thus, the default has errThresh set to 0.
 * 
 * The third argument, e2, is optional and is used to specify an expression whose value is logged when an exception
 * is thrown.
 * 
 * Catch returns the result of evaluating e1 (whose type is T1). If an exception is thrown, but
 * skipped, then null is returned.
 * 
 * Note that catch s a "blocking" call: the result of e1 will be materialized. If e1 could
 * be streamed (e.g., read(...)), when used in the context of catch, its result will be entirely
 * materialized. 
 * 
 * @jaqlExample data = [ ["a",0], ["a",1], ["b",0], ["c",0], ["c",1], ["c",2]];
 * 
 * @jaqlExample data -> write(hdfs("test"));
 * 	 
 * @jaqlExample read(hdfs("test"))
 *      -> transform catch(if ($[0] == "a") ($.badFieldAccess) // cause exceptions on ["a", 0] and ["a", 1]
 *                         else ($), 
 *                         { errThresh: 1000 }) 
 *      -> group by g = $[0] 
 *         into { g: g, 
 *                num: count($), 
 *                err: catch(if(g == "b") (g.badFieldAccess) // cause exception/null on the "b" group
 *                           else ("ok"), 
 *                           { errThresh: 1000 }) 
 *              }; 
 *[
 * {
 *   "err": "ok",
 *   "g": null,
 *   "num": 2
 *  },
 *  {
 *   "err": null,
 *   "g": "b",
 *   "num": 1
 *  },
 *  {
 *   "err": "ok",
 *   "g": "c",
 *   "num": 3
 *  }
 *]
 */
public class CatchExpr extends Expr {
	
	private boolean configured = false;
	private ExceptionHandler handler;

	public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par13
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
	protected JsonValue evalRaw(Context context) throws Exception {
		JsonValue v = null;
		if(!configured) {
			if(exprs[1] != null && exprs[1].getSchema().is(JsonType.RECORD).bool()) {
				JsonRecord opts = (JsonRecord)exprs[1].eval(context);
				JsonNumber tval = (JsonNumber)opts.get(RegisterExceptionHandler.ERROR_THRESH_FIELD_NAME);
				if(tval != null) handler = new ThresholdExceptionHandler(tval.intValue());
			} 
			if(handler == null)
				handler = JaqlUtil.getExceptionHandler();
			configured = true;
		}
		
		try {
			v = exprs[0].eval(context);
		} catch(Throwable e) {
			JsonValue c = null;
			// get the context if it exists
			if( exprs[2] != null) {
				try {
					c = exprs[2].eval(context);
				} catch(Throwable ee) {
					throw new RuntimeException(ee);
				}
			}
			handler.handleException(e, c);
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
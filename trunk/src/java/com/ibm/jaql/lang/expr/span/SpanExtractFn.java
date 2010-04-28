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
package com.ibm.jaql.lang.expr.span;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonSpan;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.SubJsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.Bool3;

/**
 * span_extract("some big string", span(2,4))
 * "me"
 * 
 * Current implementation uses SubJsonString. If the caller modifies the input string at
 * some later point, the result of this call will be invalid.
 */
public class SpanExtractFn extends Expr
{
	public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
	{
		public Descriptor()
		{
			super("span_extract", SpanExtractFn.class);
		}
	}

	/**
	 * @param exprs
	 */
	public SpanExtractFn(Expr[] exprs)
	{
		super(exprs);
	}

	
	/* (non-Javadoc)
	 * @see com.ibm.jaql.lang.expr.core.Expr#getSchema()
	 */
	@Override
	public Schema getSchema() {
		return SchemaFactory.stringOrNullSchema();
	}

	/* (non-Javadoc)
	 * @see com.ibm.jaql.lang.expr.core.Expr#evaluatesChildOnce(int)
	 */
	@Override
	public Bool3 evaluatesChildOnce(int i) {
		
		return Bool3.TRUE;
	}


	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
	 */
	public JsonString eval(final Context context) throws Exception
	{
		// get the string to extract from
		JsonString str = (JsonString) exprs[0].eval(context);
		if (str == null)
		{
			return null;
		}
		
		// get the span to define what to extract
		JsonSpan spn = (JsonSpan) exprs[1].eval(context);
		if (spn == null)
		{
			return null;
		}
		
		// check if span can be sensibly applied to str
		int begin = (int)spn.begin;
		int end = (int)spn.end;
		int len = str.length();
		if(begin > len) {
			return null;
		}
		if(end > len) {
			end = len;
		}
		
		// get the substring... note that SubJsonString assumes that str is immutable
		SubJsonString sub = new SubJsonString();
		str.substring(sub, begin, end);
		return sub;
	}
}
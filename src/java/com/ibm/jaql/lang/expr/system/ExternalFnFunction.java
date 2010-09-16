/*
 * Copyright (C) IBM Corp. 2010.
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
package com.ibm.jaql.lang.expr.system;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;

import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.parser.ParseException;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.expr.function.JavaFunctionCallExpr;
import com.ibm.jaql.lang.expr.function.JsonValueParameter;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;
import com.ibm.jaql.lang.expr.function.Parameters;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.sun.org.apache.commons.logging.LogFactory;

/**
 * value that stores a function implemented as a External Call UDF.
 */
public class ExternalFnFunction extends Function {

	private static final Expr[] NO_ARGS = new Expr[0];
	
	/** Descriptor of the function formal parameters */
	private JsonValueParameters parameters = null;
	
	/** Options to define an external process */
	private JsonRecord rec;
	
	/** Data structure that holds arguments for evaluation/inlining */
	private Expr[] args = NO_ARGS;

	
	public ExternalFnFunction(JsonRecord rec) {
		this.rec = rec;
	}
	
	public JsonRecord getExternalOpts() {
		return rec;
	}


	/* (non-Javadoc)
	 * @see com.ibm.jaql.lang.expr.function.Function#formatError(java.lang.String)
	 */
	@Override
	public String formatError(String msg) {
		return "In call of External UDF" + rec.get(new JsonString("cmd")) + ":" + msg;
	}

	/* (non-Javadoc)
	 * @see com.ibm.jaql.lang.expr.function.Function#getCopy(com.ibm.jaql.json.type.JsonValue)
	 */
	@Override
	public Function getCopy(JsonValue target) {
		return new ExternalFnFunction(rec);
	}

	/* (non-Javadoc)
	 * @see com.ibm.jaql.lang.expr.function.Function#getImmutableCopy()
	 */
	@Override
	public Function getImmutableCopy() {
		return new ExternalFnFunction(rec);
	}

	/* (non-Javadoc)
	 * @see com.ibm.jaql.lang.expr.function.Function#getParameters()
	 */
	@Override
	public Parameters<?> getParameters() {
		if (parameters == null) {
			parameters = new JsonValueParameters(new JsonValueParameter("arg", SchemaFactory.anySchema(), true));
		}
		return parameters;
	}

	/* (non-Javadoc)
	 * @see com.ibm.jaql.lang.expr.function.Function#inline(boolean)
	 */
	@Override
	public Expr inline(boolean forEval) {
		if (forEval) {
	      // cloning necessary because object construction changes parent field in expr's
	      Expr[] clonedArgs = new Expr[args.length];
	      VarMap varMap = new VarMap();
	      for (int i=0; i<args.length; i++)
	      {
	        HashSet<Var> vars = args[i].getCapturedVars();
	        for (Var v : vars) 
	        {
	          varMap.put(v, v);
	        }
	        clonedArgs[i] = args[i].clone(varMap);
	      }
	      return new ExternalFunctionCallExpr(rec, new ArrayList<Expr>(Arrays.asList(clonedArgs)));
	    }
	    return new ExternalFunctionCallExpr(rec, new ArrayList<Expr> (Arrays.asList(args)));
	}

	/* (non-Javadoc)
	 * @see com.ibm.jaql.lang.expr.function.Function#prepare(int)
	 */
	@Override
	protected void prepare(int numArgs) {
		// TODO: Add support for init external process in udf's
		if (args.length != numArgs) {
			args = new Expr[numArgs];
		}

	}

	/* (non-Javadoc)
	 * @see com.ibm.jaql.lang.expr.function.Function#setArgument(int, com.ibm.jaql.json.type.JsonValue)
	 */
	@Override
	protected void setArgument(int pos, JsonValue value) {
		args[pos] = new ConstExpr(value);

	}

	/* (non-Javadoc)
	 * @see com.ibm.jaql.lang.expr.function.Function#setArgument(int, com.ibm.jaql.json.util.JsonIterator)
	 */
	@Override
	protected void setArgument(int pos, JsonIterator it) {
		// TODO avoid copying when possible
		try {
	      SpilledJsonArray a = new SpilledJsonArray();
	      a.addCopyAll(it);
	      args[pos] = new ConstExpr(a);
	    } catch (Exception e) {
	      throw JaqlUtil.rethrow(e);
	    }

	}

	/* (non-Javadoc)
	 * @see com.ibm.jaql.lang.expr.function.Function#setArgument(int, com.ibm.jaql.lang.expr.core.Expr)
	 */
	@Override
	protected void setArgument(int pos, Expr expr) {
		args[pos] = expr;
	}

	/* (non-Javadoc)
	 * @see com.ibm.jaql.lang.expr.function.Function#setDefault(int)
	 */
	@Override
	protected void setDefault(int pos) {
		throw new UnsupportedOperationException();
	}

}

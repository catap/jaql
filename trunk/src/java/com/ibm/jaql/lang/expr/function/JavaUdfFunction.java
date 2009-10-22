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
package com.ibm.jaql.lang.expr.function;

import java.util.Arrays;
import java.util.HashSet;

import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.ClassLoaderMgr;

/** 
 * Value that stores a function implemented as a Java UDF.
 */
public class JavaUdfFunction extends Function 
{
	private static final Expr[] NO_ARGS = new Expr[0];

  /** Data structure that holds arguments for evaluation/inlining */
	private Expr[] args = NO_ARGS;
	
	/** Descriptor of the function formal parameters */
  private JsonValueParameters parameters = null;
	
  /** Implementing class */
  private Class<?> c;
	
  
  // -- construction ------------------------------------------------------------------------------

  /** Construct from the specified class. */
	public JavaUdfFunction(Class<?> c) {
		this.c = c;
	}
	
	/** Construct from the class of the specified name. */
	public JavaUdfFunction(String cls) {
		c = ClassLoaderMgr.resolveClass(cls.toString());
	}

	
	// -- self-descrtiption -------------------------------------------------------------------------
	
	@Override
	public Parameters<?> getParameters() {
	  if (parameters == null) {
	    parameters = new JsonValueParameters(new JsonValueParameter("arg", SchemaFactory.anySchema(), true));
	    // Notice: Type inference not possible because the argument at the same position in different
	    // eval methods could have different types.
	    // TODO: Infer number of arguments from eval method
	  }
	  return parameters;
	}


	/** Returns the class that contains this function's implementation */
	public Class<?> getImplementingClass() {
    return c;
  }
	 
	@Override
  protected String formatError(String msg)
  {
    return "In call of Java UDF " + c.getName() + ": " + msg;
  }

	
  // -- evaluation / inlining ---------------------------------------------------------------------
	
	@Override
  public void prepare(int numArgs) {
    //TODO: Add support for init function in udf's
    if (args.length != numArgs)
    {
      args = new Expr[numArgs];
    }
  }

  @Override
  protected void setArgument(int pos, JsonValue value) {
    args[pos] = new ConstExpr(value);
  }

  @Override
  protected void setArgument(int pos, JsonIterator it) {
    // TODO avoid copying when possible
    try 
    {
      SpilledJsonArray a = new SpilledJsonArray();
      a.addCopyAll(it);
      args[pos] = new ConstExpr(a);
    }
    catch (Exception e)
    {
      throw JaqlUtil.rethrow(e);
    }
  }

  @Override
  protected void setArgument(int pos, Expr expr) {
    args[pos] = expr;
  }

  @Override
  protected void setDefault(int pos) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public Expr inline(boolean forEval) {
    if (forEval)
    {
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
      return new JavaFunctionCallExpr(c, Arrays.asList(clonedArgs));
    }
    return new JavaFunctionCallExpr(c, Arrays.asList(args));
  }
	
	// -- copying -----------------------------------------------------------------------------------
	
	@Override
	public Function getCopy(JsonValue target) {
		return new JavaUdfFunction(c);
	}

	@Override
	public Function getImmutableCopy() {
		return new JavaUdfFunction(c);
	}
}

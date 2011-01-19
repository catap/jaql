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
package com.ibm.jaql.lang;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.top.QueryExpr;


/**
 * A fully parsed script. Statements are evaluated as results are requested.
 * Non-result statements (like assignments) are evaluated before the result is returned.  
 * Non-result statements at the end of the script are not evaluated! // TODO: what to do here? 
 */
public class ParsedJaql
{
  protected Jaql jaql;
  protected Env env;
  protected Context context;
  protected ArrayList<Expr> exprs = new ArrayList<Expr>();
  protected int numResults = 0;
  protected int index = 0;
  protected int resultsLeft = 0;
  protected Schema curSchema;
  protected List<Var> externalVars;
  protected HashMap<Var,JsonValue> externalDefaults = new HashMap<Var, JsonValue>();
  
  /** Parse but don't evaluate all the statements in the current parser input */
  public ParsedJaql(Jaql jaql, Env env, Context context) throws Exception
  {
    this.jaql = jaql;
    this.env = env;
    this.context = context;
    Expr expr;
    while( (expr = jaql.prepareNext()) != null )
    {
      exprs.add(expr);
      if( expr instanceof QueryExpr )
      {
        numResults++;
      }
    }
    resultsLeft = numResults;
    
    externalVars = env.globals.listExternal(false);
    for( Var v: externalVars )
    {
      if( v.isDefined() )
      {
        externalDefaults.put(v, v.getValue(context));
      }
    }
  }

  /** Return a list of the external variables.
   * The variables should be considered immutable.
   * *** DO NOT CALL THE SETTERS! ****  
   * Use setExternalVariable to modify.
   */
  public List<Var> getExternalVariables() // TODO: make a wrapper to hide the vars? just expose name, type, required to be set/isdefined
  {
    return externalVars;
  }
  
  /** Set an external variable to a value. */
  public void setExternalVariable(String name, JsonValue value)
  {
    Var var = env.globals.findVar(name);
    if( var == null )
    {
      throw new IllegalArgumentException("unknown variable: "+name);
    }
    if( ! var.isMutable() )
    {
      throw new IllegalArgumentException("variable is not external: "+name);
    }
    if( !var.getSchema().matchesUnsafe(value) ) // TODO: this check should be in var.setValue
    {
      throw new ClassCastException("incompatible type variable "+name+":"+var.getSchema()+" := "+value);
    }
    var.setValue(value);
  }

  /** Is it safe to call eval() or iter() */
  public boolean hasMoreResults() throws Exception
  {
    processNonResults();
    return resultsLeft > 0;
  }

  /** Release any resources held by this script.  It may be reopended. */
  public void close() throws Exception
  {
    // TODO: anything else to release?  Add method on context for this?
    context.reset();
    // TODO: reset all mutable variables to their initial value / undefined
    for( Var v: externalVars )
    {
      if( externalDefaults.containsKey(v) )
      {
        v.setValue( externalDefaults.get(v) );
      }
      else
      {
        v.undefine();
      }
    }

  }
  
  /** Restart this script from the beginning. 
   * It does NOT reset variables, so defaults will not be reinstated.
   * Closing a script before opening it will reset variables, however. 
   */
  public void open() throws Exception
  {
    index = 0;
    resultsLeft = numResults;
    processNonResults();
  }
  
  /** Close the previous result and evaluate the next. */
  public JsonValue eval() throws Exception 
  {
    processNonResults();
    if( index < exprs.size() )
    {
      Expr expr = exprs.get(index++);
      resultsLeft--;
      curSchema = expr.getSchema();
      return expr.eval(context);
    }
    else
    {
      throw new IndexOutOfBoundsException("Too many calls to evaluate: "+exprs.size());
    }
  }

  /** Close the previous result and open an iterator over the next result. */
  public JsonIterator iter() throws Exception 
  {
    processNonResults();
    if( index < exprs.size() )
    {
      Expr expr = exprs.get(index++);
      resultsLeft--;
      curSchema = expr.getSchema();
      return expr.iter(context);
    }
    else
    {
      throw new IndexOutOfBoundsException("Too many calls to iterate: "+exprs.size());
    }
  }

  /** Invoke a function and return the full result. */
  public JsonValue eval(String fnName, FunctionArgs args) throws Exception 
  {
    processNonResults();
    Expr expr = jaql.prepareFunctionCall(fnName, args);
    curSchema = expr.getSchema();
    return expr.eval(context);
  }

  /** Invoke a function and iterate over its result. */
  public JsonIterator iter(String fnName, FunctionArgs args) throws Exception 
  {
    processNonResults();
    Expr expr = jaql.prepareFunctionCall(fnName, args);
    curSchema = expr.getSchema();
    return expr.iter(context);
  }


  /** Process any non-result set statements (like AssignExpr). */
  protected void processNonResults() throws Exception
  {
    context.reset();
    while( index < exprs.size() )
    {
      Expr expr = exprs.get(index);
      if( expr instanceof QueryExpr )
      {
        break;
      }
      index++;
      expr.eval(context);
      context.reset();
    }
  }

  /**
   * Returns the schema of the most recently returned value from eval or iter
   */
  public Schema currentSchema()
  {
    return curSchema;
  }

  /**
   * Return the schema of the next result from eval or iter
   * @throws Exception 
   */
  public Schema getNextSchema() throws Exception
  {
    processNonResults();
    if( index < exprs.size() )
    {
      Expr expr = exprs.get(index);
      return expr.getSchema();
    }
    else
    {
      throw new IndexOutOfBoundsException("Too many calls to getNextSchema: "+exprs.size());
    }
  }
}

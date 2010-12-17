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
package com.ibm.jaql.lang.expr.core;

import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.Bool3;

/**
 * This function is used internally during the rewriting of tee().  
 * It is not intended for general use.
 * 
 * e -> tag(i)
 * 
 * Exactly the same as:
 *   e -> transform [i,$] 
 */
public class TagFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("tag", TagFn.class);
    }
  }
  
  public TagFn(Expr... exprs)
  {
    super(exprs);
  }

  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }
  
  @Override
  public boolean isMappable(int i)
  {
    return i == 0;
  }

// TODO:
//  @Override
//  public Schema getSchema()

  @Override
  protected JsonIterator iterRaw(final Context context) throws Exception
  {
    JsonNumber jnum = (JsonNumber)exprs[1].eval(context);
    final BufferedJsonArray pair = new BufferedJsonArray(2);
    pair.set(0, jnum);
    
    return new JsonIterator(pair)
    {
      JsonIterator iter = exprs[0].iter(context);
      
      @Override
      protected boolean moveNextRaw() throws Exception
      {
        if( iter.moveNext() )
        {
          pair.set(1, iter.current());
          return true;
        }
        return false;
      }
    };
  }
 
  /** 
   * Replace this function by its definition:
   *   e -> tag(i)
   * ==>
   *   e -> transform [i,$] 
   */
  public Expr expand(Env env)
  {
    Var var = env.makeVar("$");
    Expr expr = new ArrayExpr(exprs[1], new VarExpr(var));
    expr = new TransformExpr(var, exprs[0], expr);
    if( parent != null )
    {
      replaceInParent(expr);
    }
    return expr;
  }
}

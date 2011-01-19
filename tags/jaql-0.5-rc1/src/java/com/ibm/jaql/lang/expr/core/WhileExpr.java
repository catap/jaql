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

import java.util.HashSet;

import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.util.FastPrinter;

/**
 * Perform repeated computation.  In procedural code this would be:
 *   T t = init;
 *   while( cond(t) )
 *      t = body(t)
 *   return t
 *   
 * loop( init: T, cond: fn(T): bool, body: fn(T): T ): T 
 */
public class WhileExpr extends Expr
{
  public WhileExpr(Expr... args)
  {
    super(args);
  }

  public WhileExpr(BindingExpr init, Expr cond, Expr body)
  {
    super(init, cond, body);
  }

  
  public BindingExpr binding()
  {
    return (BindingExpr)exprs[0];
  }

  public Expr condition()
  {
    return exprs[1];
  }

  public Expr body()
  {
    return exprs[2];
  }

  @Override
  public void decompile(FastPrinter exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print( "\n" );
    exprText.print( kw("while") );
    exprText.print( "( " );
    BindingExpr b = binding();
    b.decompile(exprText, capturedVars);
    exprText.print( ",\n       " );
    condition().decompile(exprText, capturedVars);
    exprText.print( ")\n(\n" );
    body().decompile(exprText, capturedVars);
    exprText.print( ")\n" );
    capturedVars.remove(b.var);
  }

  
  @Override
  public JsonValue eval(final Context context) throws Exception
  {
    BindingExpr b = (BindingExpr)exprs[0];
    Expr cond = exprs[1];
    Expr body = exprs[2];
    Var var = b.var;
    
    // T t = init;
    JsonValue t1 = b.inExpr().eval(context);
    JsonValue t2 = null;
    
    // while( cond(t) )
    while( true )
    {
      var.setValue(t1);
      JsonBool jb = (JsonBool)cond.eval(context);
      if( jb == null || jb.get() == false )
      {
        break;
      }
      
      // t = body(t)
      JsonValue t3 = body.eval(context);
      // We need the copy because t might refer to t1
      t2 = JsonUtil.getCopy(t3, t2);
      
      // Swap the pointers so that t2 will be passed in next time, and t1 becomes a free buffer.
      t3 = t1;
      t1 = t2;
      t2 = t3;
    }
    
    // return t
    return t1;
  }
}

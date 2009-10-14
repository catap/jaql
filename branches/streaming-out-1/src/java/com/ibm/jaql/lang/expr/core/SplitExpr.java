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
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.util.JaqlUtil;


public class SplitExpr extends Expr
{

  public SplitExpr(Expr[] inputs)
  {
    super(inputs);
  }
  
  /**
   * @param inputs BindingExpr, IfExpr+
   */
  public SplitExpr(ArrayList<Expr> inputs)
  {
    super(inputs);
  }

  public BindingExpr binding()
  {
    return (BindingExpr)exprs[0];
  }
  
  @Override
  public Schema getSchema()
  {
    return SchemaFactory.nullSchema();
  }

  @Override
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    BindingExpr b = binding();
    b.exprs[0].decompile(exprText, capturedVars);
    exprText.print("\n-> split each ");
    exprText.print(b.var.taggedName());
    for(int i = 1 ; i < exprs.length ; i++)
    {
      exprs[i].decompile(exprText, capturedVars);
      exprText.println();
    }
    capturedVars.remove(b.var);
  }

  @Override
  public JsonValue eval(final Context context) throws Exception // TODO: sinks should not return anything
  {
    // TODO: this and tee could/should fork threads to eval branches without temping
    BindingExpr b = (BindingExpr)exprs[0];
    int i;
    final int n = exprs.length - 1;
    // TODO: we can simplify when all the predicates are constant (not per input item) 
    final SpilledJsonArray[] temps = new SpilledJsonArray[n];
    final IfExpr[] ifs = new IfExpr[n];
    for( i = 0 ; i < n ; i++ )
    {
      temps[i] = new SpilledJsonArray(); // TODO: memory
      ifs[i] = (IfExpr)exprs[i+1];
    }
    
    JsonIterator iter = b.inExpr().iter(context);
    for (JsonValue value : iter)
    {
      b.var.setValue(value);
      for( i = 0 ; i < n ; i++ )
      {
        if( JaqlUtil.ebv(ifs[i].testExpr().eval(context)) )
        {
          temps[i].addCopy(value);
          break;
        }
      }
    }

    JsonValue[] args = new JsonValue[1];
    for( i = 0 ; i < n ; i++ )
    {
      //if( ! temps[i].isEmpty() ) // TODO: should empty flow?
      {
        args[0]= temps[i];
        Function f = (Function)ifs[i].trueExpr().eval(context);
        f.setArguments(args);
        f.eval(context);
      }
    }

    return null;
  }

}

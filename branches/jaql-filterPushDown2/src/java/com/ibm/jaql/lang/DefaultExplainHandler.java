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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Iterator;

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.io.RegisterAdapterExpr;
import com.ibm.jaql.lang.expr.top.AssignExpr;
import com.ibm.jaql.lang.expr.top.ExplainExpr;
import com.ibm.jaql.lang.expr.top.QueryExpr;

public class DefaultExplainHandler extends ExplainHandler
{
  protected PrintStream out;
  
  public DefaultExplainHandler(PrintStream out)
  {
    this.out = out;
  }
  
  @Override
  public Expr explain(Expr expr) throws Exception
  {
    String stmt = decompile(expr);
    out.println(stmt);
    if( expr instanceof AssignExpr ||
        expr instanceof QueryExpr && expr.child(0) instanceof RegisterAdapterExpr ) // HACK: if we don't register, explain will change or bomb. This will go away with the registry.
    {
      return expr;
    }
    return null;
    // return new ConstExpr(new JsonString(query));
  }

  public static String decompile(Expr expr) throws Exception
  {
    if( expr instanceof ExplainExpr )
    {
      expr = expr.child(0);
    }
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    PrintStream exprText = new PrintStream(outStream);
    HashSet<Var> capturedVars = new HashSet<Var>();
    expr.decompile(exprText, capturedVars);
    if( !capturedVars.isEmpty() )
    {
      Iterator<Var> iter = capturedVars.iterator();
      while (iter.hasNext())
      {
        Var var = iter.next();
        if (var.isGlobal())
        {
          iter.remove();
        }
      }
      if( !capturedVars.isEmpty() )
      {
        System.err.println("Invalid query... Undefined variables:");
        for (Var key : capturedVars)
        {
          System.err.println(key.taggedName());
        }
        System.err.println(outStream.toString());
        throw new RuntimeException("undefined variables");
      }
    }
    exprText.print(";");
    String stmt = outStream.toString();
    return stmt;
  }
}

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
package com.ibm.jaql.lang.expr.top;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Iterator;

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;

/**
 * 
 */
public class ExplainExpr extends TopExpr
{
  /**
   * boolean explain expr
   * 
   * @param exprs
   */
  public ExplainExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr
   */
  public ExplainExpr(Expr expr)
  {
    super(new Expr[]{expr});
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print("explain ");
    exprs[0].decompile(exprText, capturedVars);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonString eval(Context context) throws Exception
  {
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    PrintStream exprText = new PrintStream(outStream);
    HashSet<Var> capturedVars = new HashSet<Var>();
    exprs[0].decompile(exprText, capturedVars);
    if (!capturedVars.isEmpty()) // FIXME: change root expr from NoopExpr to QueryStmt
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
      if (!capturedVars.isEmpty())
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
    // assert capturedVars.isEmpty();
    String query = outStream.toString();
    return new JsonString(query);
  }
}

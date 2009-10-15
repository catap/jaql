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

import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.AscDescJsonComparator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JsonComparator;
import com.ibm.jaql.lang.core.Var;


public class CmpArray extends CmpExpr
{
  /**
   * Order by a list of expressions and produce an array key.
   *
   * @param exprs[] CmpSpec[]
   */
  public CmpArray(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * 
   * @param spec
   */
  public CmpArray(ArrayList<CmpSpec> spec)
  {
    super(spec);
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
    exprText.print(kw("cmp") + " [");
    String sep = " ";
    for(Expr e: exprs)
    {
      exprText.print(sep);      
      e.decompile(exprText, capturedVars);
      sep = ", ";
    }
    exprText.print(" ]");
  }

  protected BufferedJsonArray array; 

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonValue eval(Context context) throws Exception
  {
    if( array == null )
    {
      array = new BufferedJsonArray(exprs.length);
    }
    for(int i = 0 ; i < exprs.length ; i++)
    {
      JsonValue value = exprs[i].eval(context);
      array.set(i, value);
    }
    return array;
  }
  
  public JsonComparator getComparator(Context context)
  {
    int n = exprs.length;
    boolean[] asc = new boolean[n];
    for (int i = 0; i < exprs.length; i++)
    {
      CmpSpec o = (CmpSpec) exprs[i];
      // TODO: should use o.getComparator(context)
      asc[i] = (o.order == CmpSpec.Order.ASC);
    }
    return new AscDescJsonComparator(asc);
  }
}

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

import com.ibm.jaql.json.type.FixedJArray;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.util.AscDescItemComparator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JComparator;
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
    exprText.print("cmp [");
    String sep = " ";
    for(Expr e: exprs)
    {
      exprText.print(sep);      
      e.decompile(exprText, capturedVars);
      sep = ", ";
    }
    exprText.print(" ]");
  }

  protected FixedJArray array; 
  protected Item arrayItem;

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(Context context) throws Exception
  {
    if( arrayItem == null )
    {
      array = new FixedJArray(exprs.length);
      arrayItem = new Item(array);
    }
    for(int i = 0 ; i < exprs.length ; i++)
    {
      Item item = exprs[i].eval(context);
      array.set(i, item);
    }
    return arrayItem;
  }
  
  public JComparator getComparator(Context context)
  {
    int n = exprs.length;
    boolean[] asc = new boolean[n];
    for (int i = 0; i < exprs.length; i++)
    {
      CmpSpec o = (CmpSpec) exprs[i];
      // TODO: should use o.getComparator(context)
      asc[i] = (o.order == CmpSpec.Order.ASC);
    }
    return new AscDescItemComparator(asc);
  }
}

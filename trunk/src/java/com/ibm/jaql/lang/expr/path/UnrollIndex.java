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
package com.ibm.jaql.lang.expr.path;

import java.io.PrintStream;
import java.util.HashSet;

import com.ibm.jaql.json.type.FixedJArray;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JNumber;
import com.ibm.jaql.json.type.SpillJArray;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;


public class UnrollIndex extends UnrollStep
{
  /**
   * @param exprs
   */
  public UnrollIndex(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param index
   */
  public UnrollIndex(Expr index)
  {
    super(index);
  }


  /**
   * 
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
  throws Exception
  {
    exprText.print("[");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print("]");
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.PathExpr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public Item expand(Context context, Item toExpand) throws Exception
  {
    JArray arr = (JArray)toExpand.get();
    if( arr == null )
    {
      return null;
    }
    JNumber index = (JNumber)exprs[0].eval(context).get();
    if( index == null )
    {
      return null;
    }
    long k = index.longValueExact();
    Iter iter = arr.iter();
    Item item;
    Item hole = null;
    JArray out;
    if( arr instanceof FixedJArray )
    {
      FixedJArray fixed = new FixedJArray((int)arr.count()); // TODO: memory
      out = fixed;
      long i = 0;
      while( (item = iter.next()) != null )
      {
        if( i == k )
        {
          item = hole = new Item(item.get()); // TODO: memory
        }
        fixed.set((int)i,item); // TODO: shouldn't add() be used here? 
        i++;
      }
    }
    else
    {
      SpillJArray spill = new SpillJArray(); // TODO: memory
      out = spill;
      long i = 0;
      while( (item = iter.next()) != null )
      {
        if( i == k )
        {
          item = hole = new Item(item.get()); // TODO: memory
        }
        spill.addCopy(item);
        i++;
      }
    }
    toExpand.set(out);
    return hole;
  }
}

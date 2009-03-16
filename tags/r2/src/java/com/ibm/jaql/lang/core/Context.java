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
package com.ibm.jaql.lang.core;

import java.util.HashMap;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.IntArray;
import com.ibm.jaql.util.PagedFile;
import com.ibm.jaql.util.SpillFile;

/**
 * 
 */
public class Context
{
  Item[]   stack    = new Item[16];
  //  Item[] temps = new Item[2];
  int      basep    = 0;
  IntArray frames   = new IntArray();
  int      topIndex = 0;

  /**
   * 
   */
  public Context()
  {
  }

  /**
   * @param var
   * @param value
   */
  public void setVar(Var var, Item value)
  {
    //System.out.print("set var "+var.name+": ");
    //Util.print(System.out, value, 0);
    //System.out.println();

    int i = basep + var.index;
    if (i >= stack.length)
    {
      Item[] newStk = new Item[i + 16];
      System.arraycopy(stack, 0, newStk, 0, stack.length);
      stack = newStk;
    }
    if (var.index >= topIndex) // TODO: the stack management needs improvement
    {
      topIndex = var.index + 1;
    }
    stack[i] = value;
  }

  //  public void setVar(Var var, Iter iter) throws Exception
  //  {
  //    setVar(var, temp(var, iter));
  //  }

  /**
   * @param var
   * @return
   * @throws Exception
   */
  public Item getValue(Var var) throws Exception
  {
    if (var.expr != null) // global var
    {
      if (var.value != null)
      {
        return var.value;
      }
      Context gctx = JaqlUtil.getSessionContext();
      Item value = var.expr.eval(gctx);
      return value;
    }
    // local var
    Item x = stack[basep + var.index];
    return x;
  }

  /**
   * @param var
   * @return
   * @throws Exception
   */
  public Iter getIter(Var var) throws Exception
  {
    if (var.expr != null && var.value == null)
    {
      Context gctx = JaqlUtil.getSessionContext();
      return var.expr.iter(gctx);
    }

    Item x = getValue(var);
    // cast error intentionally possible
    JArray array = (JArray) x.get();
    if (array == null)
    {
      return Iter.nil;
    }
    return array.iter();
  }

  //  // This always returns an Array Item
  //  public Item getTempTable(Var var)
  //  {
  //    int i = basep + var.index;
  //    if( i >= temps.length )
  //    {
  //      Item[] temps2 = new Item[i + 10];
  //      System.arraycopy(temps, 0, temps2, 0, temps.length);
  //      temps = temps2;
  //    }
  //    Item item = temps[basep + var.index];
  //    if( temps == null )
  //    {
  //      ByteJArray table = new ByteJArray();
  //      item = new Item(table);
  //      temps[basep + var.index] = item;
  //    }
  //    return item;
  //  }

  //  protected Item temp(Var var, Iter iter) throws Exception
  //  {
  //    Item item = getTempTable(var);
  //    ByteJArray table = (ByteJArray)item.get();
  //    table.set(iter);
  //    return item;
  //  }

  /**
   * 
   */
  public void reset()
  {
    basep = 0;
    for (int i = 0; i < stack.length; i++)
    {
      stack[i] = null;
    }
    JaqlUtil.getQueryPageFile().clear();
  }

  /**
   * 
   */
  public void push()
  {
    frames.add(basep);
    basep += topIndex;
  }

  /**
   * 
   */
  public void pop()
  {
    basep = frames.pop();
  }

  /**
   * 
   */
  protected HashMap<Expr, Item> tempItems = new HashMap<Expr, Item>();
  // TODO: use this or temp vars or something else?
  /**
   * @param expr
   * @return
   */
  public Item getTemp(Expr expr)
  {
    // FIXME: this needs to be fixed for recursion
    Item item = tempItems.get(expr);
    if (item == null)
    {
      item = new Item();
      tempItems.put(expr, item);
    }
    return item;
  }

  /**
   * @param item
   * @return
   * @throws Exception
   */
  public Item makeSessionGlobal(Item item) throws Exception
  {
    Item global = new Item();
    if (item.isAtom())
    {
      global.copy(item);
    }
    else
    {
      PagedFile pf = JaqlUtil.getSessionPageFile();
      SpillFile sf = new SpillFile(pf);
      item.write(sf);
      global.readFields(sf.getInput());
    }
    return global;
  }

}

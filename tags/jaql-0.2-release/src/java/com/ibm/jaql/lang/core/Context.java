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

import java.util.ArrayList;
import java.util.HashMap;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.SpillJArray;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.IntArray;
import com.ibm.jaql.util.PagedFile;
import com.ibm.jaql.util.SpillFile;

/** Run-time context, i.e., values for the variables in the environment.
 * 
 */
public class Context
{
  Item[]   stack    = new Item[16]; // contains the values, extended if needed
  //  Item[] temps = new Item[2];
  int      basep    = 0;
  IntArray frames   = new IntArray();
  int      topIndex = 0;
  
  // PyModule pyModule;

  /**
   * Path expressions pass their context down to child expressions using this value.
   */
  public Item pathInput; // TODO: eliminate
  

  /**
   * 
   */
  public Context()
  {
    if( JaqlUtil.getSessionContext() == null )
    {
//      PySystemState systemState = Py.getSystemState();
//      if (systemState == null)
//      {
//        systemState = new PySystemState();
//      }
//      Py.setSystemState(systemState);
//      pyModule = new PyModule("jaqlMain", new PyStringMap());
    }
  }

  // public PyModule getPyModule() { return JaqlUtil.getSessionContext().pyModule; }
  
  /** Puts a variable onto the stack. The position of the variable is determined by its
   * {@link Var#index} field.
   * 
   * @param var
   * @param value
   */
  public void setVar(Var var, Item value)
  {
    //System.out.print("set var "+var.name+": ");
    //Util.print(System.out, value, 0);
    //System.out.println();

    assert value != null;
    if( var == Var.unused )
    {
      return;
    }
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
    var.iter = null;
  }

  /**
   * Set var to the value of an iter.  The var must have one reference that makes one-pass. 
   * @param var
   * @param iter
   */
  public void setVar(Var var, Iter iter)
  {
    // TODO: didn't bother with the stack until we decide we need recursion.
    var.iter = iter;
  }


  //  public void setVar(Var var, Iter iter) throws Exception
  //  {
  //    setVar(var, temp(var, iter));
  //  }

  /** Returns the current value of the specified variable.
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
    Item item = stack[basep + var.index];
    if( var.iter != null )
    {
      if( item == null )
      {
        stack[basep + var.index] = item = new Item();
      }
      SpillJArray arr;
      if( item.get() instanceof SpillJArray )
      {
        arr = (SpillJArray)item.get();
        arr.clear();
      }
      else
      {
        arr = new SpillJArray();
        item.set(arr);
      }
      arr.set(var.iter);
    }
    return item;
  }

  /** Returns an iterator over the current value of the variable, which must be
   * a {@link JArray}.
   * 
   * @param var
   * @return
   * @throws Exception
   * @throws ClassCastException if the value represented by the specified variable is not
   * assignable to a {@link JArray}
   */
  public Iter getIter(Var var) throws Exception
  {
    if( var.expr != null && var.value == null ) // global variable
    {
      Context gctx = JaqlUtil.getSessionContext();
      return var.expr.iter(gctx);
    }
    if( var.iter != null )
    {
      return var.iter;
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

  /** Clears the context.
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

  /** UNUSED: Pushes the current context onto a stack and provides a new empty context. Subsequent 
   * calls to {@link #getValue(Var)}, {@link #getIter(Var)}, and {@link #setVar(Var, Item)} 
   * will refer to the new context. 
   */
  public void push()
  {
    frames.add(basep);
    basep += topIndex;
  }

  /** UNUSED: Removes the current context from the stack. All information within this context is lost.
   * Subsequent calls to {@link #getValue(Var)}, {@link #getIter(Var)}, and 
   * {@link #setVar(Var, Item)} will refer to the context, taken from the the stack.   
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
  // TODO: where is this map cleared?
  
  /** If not already cached, evaluates the given expression and caches its result. Otherwise,
   * returns the cached result. 
   * 
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

  /** UNUSED 
   * @param item
   * @return
   * @throws Exception
   */
  public Item makeSessionGlobal(Item item) throws Exception
  {
    Item global = new Item();
    if (item.isAtom())
    {
      global.copy(item); // TODO: copy should take a pagefile
    }
    else // FIXME: this is not doing what it is supposed to do 
    {
      PagedFile pf = JaqlUtil.getSessionPageFile(); // TODO: this is BROKEN!
      SpillFile sf = new SpillFile(pf);
      item.write(sf);
      global.readFields(sf.getInput());      
    }
    return global;
  }

  protected ArrayList<Runnable> atQueryEnd = new ArrayList<Runnable>();
  public void doAtQueryEnd(Runnable task)
  {
    atQueryEnd.add(task);
  }
  
  public void endQuery()
  {
    reset();
    for(Runnable task: atQueryEnd)
    {
      try
      {
        task.run();
      }
      catch(Exception e)
      {
        e.printStackTrace(); // TODO: log
      }
    }
    atQueryEnd.clear();
  }
}

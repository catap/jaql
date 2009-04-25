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

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.SpillJArray;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * 
 */
public class Var extends Object
{
  public enum Usage
  {
    EVAL(),    // Variable may be referenced multiple times (value must be stored) 
    STREAM(),  // Variable is an array that is only referenced once (value may be streamed)
    UNUSED()   // Variable is never referenced
  };
  
  public static final Var[] NO_VARS = new Var[0];
  public static final Var unused = new Var("$__unused__");

  public String             name;
  public boolean            hidden  = false;     // variable not accessible in current parse context (this could reuse Usage)
  public Var                varStack;            // Used during parsing for vars of the same name; contains the a list of previous definitions of this variable
  public Item               value;               // The variable's full value
  public Iter               iter;                // The variable's lazy value; only one of value or iter is non-null
  public Expr               expr;                // only for global variables
  public Usage              usage = Usage.EVAL;

  /**
   * @param name
   */
  public Var(String name)
  {
    this.name = name;
  }

  /**
   * @return
   */
  public String name()
  {
    return name;
  }

  /**
   * @return
   */
  public boolean isGlobal()
  {
    return expr != null;
  }

  /*** Returns the name of this variable without the leading character, which is
   * assumed to equal $.
   */
  public String nameAsField()
  {
    return name.substring(1);
  }

  /**
   * @param varMap
   * @return
   */
  public Var clone(VarMap varMap)
  {
    Var v = new Var(name);
    v.value = value; // TODO: is it safe to share the value?
    v.usage = usage;
    // It is NOT safe to share an iter unless one var is never evaluated.
    if( iter != null )
    {
      throw new RuntimeException("cannot clone variable with Iter");
    }
    if (expr != null)
    {
      // TODO: do we need to clone a Var with an expr? Do we need to clone the Expr?
      throw new RuntimeException("cannot clone variable with Expr");
      // v.expr = expr.clone(varMap); // TODO: could we share the expr? 
    }
    return v;
  }

  /**
   * 
   * @param value
   */
  public void set(Item value)
  {
    assert value != null;
    this.value = value;
    iter = null;
  }

  /**
   * 
   * @param iter
   */
  public void set(Iter iter)
  {
    assert iter != null;
    this.iter = iter;
    value = null;
  }
  
  /**
   * Set the variable's value to the result of the expression.
   * If the variable is unused, the expression is not evaluated.
   * If the variable is streamable and the expression is known to produce an array. 
   * 
   * @param expr
   * @param context
   * @throws Exception
   */
  public void set(Expr expr, Context context) throws Exception
  {
    // TODO: should the usage be STREAM only if it is used in an array context?
    if( usage == Usage.STREAM && expr.isArray().always() ) 
    {
      set(expr.iter(context));
    }
    else if( usage != Usage.UNUSED )
    {
      set(expr.eval(context));
    }
  }

  /**
   * Return the value of a variable.  
   * It is safe to request the value multiple times.
   * 
   * At the time of this writing, global variables are never evaluated; they are first made into
   * query-local variables.
   * 
   * @return
   * @throws Exception
   */
  public Item getValue() throws Exception
  {
    if( value != null )
    {
      assert iter == null;
      return value;
    }
    else if( iter != null )
    {
      SpillJArray arr = new SpillJArray();
      arr.setCopy(iter);
      value = new Item(arr);
      iter = null;
      return value;
    }
    else if (expr != null) // global var
    {
      Context gctx = JaqlUtil.getSessionContext();
      value = expr.eval(gctx);       // TODO: init/close calls.
      return value;
    }
    throw new NullPointerException("undefined variable: "+name);
  }
  
  /**
   * Return an iterator over the (array) value.
   * If the value is not an array, an exception is raised.
   * If this variable has STREAM usage, it is NOT safe to request the value multiple times.
   * 
   * @return
   * @throws Exception
   */
  public Iter getIter() throws Exception
  {
    if( value != null )
    {
      assert iter == null;
      JArray arr = (JArray)value.get(); // cast error intentionally possible
      if( arr == null )
      {
        return Iter.nil;
      }
      return arr.iter();
    }
    else if( iter != null )
    {
      Iter iter = this.iter;
      this.iter = null;
      if( usage == Usage.STREAM )
      {
        return iter;
      }
      else
      {
        SpillJArray arr = new SpillJArray();
        arr.setCopy(iter);
        value = new Item(arr);
        return arr.iter();
      }
    }
    else if (expr != null) // global var
    {
      Context gctx = JaqlUtil.getSessionContext();
      if( usage == Usage.STREAM )
      {
        return expr.iter(gctx);
      }
      else
      {
        value = expr.eval(gctx);
        JArray arr = (JArray)value.get(); // cast error intentionally possible
        if( arr == null )
        {
          return Iter.nil;
        }
        return arr.iter();
      }
    }
    throw new NullPointerException("undefined variable: "+name);
  }

  @Override
  public String toString()
  {
    return name + " @" + System.identityHashCode(this);
  }
}

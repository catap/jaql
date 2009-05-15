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
  public Expr               expr;                // only for global variables
  public Usage              usage = Usage.EVAL;
  
  public Object             value;               // Runtime value: Item or Iter

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
    v.usage = usage;
    // Cloning a Var does NOT clone its value!
    // It is NOT safe to share an iter unless one var is never evaluated.
    if (expr != null)
    {
      // TODO: do we need to clone a Var with an expr? Do we need to clone the Expr?
      throw new RuntimeException("cannot clone variable with Expr");
      // v.expr = expr.clone(varMap); // TODO: could we share the expr? 
    }
    return v;
  }

  @Override
  public String toString()
  {
    return name + " @" + System.identityHashCode(this);
  }

  /**
   * Unset the runtime value
   */
  public void undefine()
  {
    this.value = null;
  }
  
  /**
   * Set the runtime value.
   * 
   * @param value
   */
  public void setValue(Item value)
  {
    assert value != null;
    this.value = value;
  }

  /**
   * Set the runtime value.
   * 
   * @param var
   * @param value
   */
  public void setIter(Iter iter)
  {
    assert iter != null;
    value = iter;
  }

  /**
   * Set the variable's value to the result of the expression.
   * If the variable is unused, the expression is not evaluated.
   * If the variable is streamable and the expression is known to produce an array,
   *   the expr is evaluated lazily using an Iter. 
   * 
   * @param expr
   * @param context
   * @throws Exception
   */
  public void setEval(Expr expr, Context context) throws Exception
  {
    if( usage == Usage.STREAM && expr.isArray().always() ) 
    {
      setIter(expr.iter(context));
    }
    else if( usage != Usage.UNUSED )
    {
      setValue(expr.eval(context));
    }
  }
  
  /**
   * 
   * @param var
   * @param value
   * @throws Exception
   */
  public void setGeneral(Object value, Context context) throws Exception
  {
    if( value instanceof Item )
    {
      setValue((Item)value);
    }
    else if( value instanceof Iter )
    {
      setIter((Iter)value);
    }
    else if( value instanceof Expr )
    {
      setEval((Expr)value, context);
    }
    else
    {
      throw new InternalError("invalid variable value: "+value);
    }
  }

  
  /**
   * Get the runtime value of the variable.
   * 
   * @return
   * @throws Exception
   */
  public Item getValue(Context context) throws Exception
  {
    if( value instanceof Item )
    {
      return (Item)value;
    }
    else if( value instanceof Iter )
    {
      SpillJArray arr = new SpillJArray();
      arr.setCopy((Iter)value);
      Item v = new Item(arr);
      value = v;
      return v;
    }
    else if( expr != null ) // TODO: merge value and expr? value is run-time; expr is compile-time
    {
      Item v = expr.eval(context);
      expr = null;
      value = v;
      return v;
    }
    else if( value == null )
    {
      throw new NullPointerException("undefined variable: "+name());
    }
    throw new InternalError("bad variable value: "+name()+"="+value);
  }
  
  /**
   * Return an iterator over the (array) value.
   * If the value is not an array, an exception is raised.
   * If this variable has STREAM usage, it is NOT safe to request the value multiple times.
   * 
   * @return
   * @throws Exception
   */
  public Iter getIter(Context context) throws Exception
  {
    if( usage == Usage.STREAM && value instanceof Iter )
    {
      Iter iter = (Iter)value;
      value = null; // set undefined
      return iter;
    }
    Item v = getValue(context);
    JArray arr = (JArray)v.get(); // cast error intentionally possible
    if( arr == null )
    {
      return Iter.nil;
    }
    return arr.iter();
  }

}

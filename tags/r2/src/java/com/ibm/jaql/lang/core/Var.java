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

import java.lang.reflect.UndeclaredThrowableException;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.lang.expr.core.Expr;

/**
 * 
 */
public class Var
{
  public static final Var[] NO_VARS = new Var[0];

  public String             name;
  public boolean            hidden  = false;
  public Var                varStack;            // Used during parsing for vars of the same name
  public int                index;               // -1 for global variables; stack index for local variables 
  public Expr               expr;                // only for global variables
  public Item               value;               // only for global variables

  /**
   * @param name
   * @param index
   */
  public Var(String name, int index)
  {
    this.name = name;
    this.index = index;
  }

  /**
   * @return
   */
  public boolean isGlobal()
  {
    return expr != null;
  }

  /**
   * @return
   */
  public String name()
  {
    return name;
  }

  /**
   * @param varMap
   * @return
   */
  public Var clone(VarMap varMap)
  {
    Var v = varMap.env().makeVar(name);
    v.hidden = hidden;
    v.varStack = varStack;
    varStack = v;
    if (expr != null)
    {
      v.expr = expr.clone(varMap);
    }
    if (value != null)
    {
      v.value = new Item();
      try
      {
        v.value.copy(value);
      }
      catch (Exception e)
      {
        throw new UndeclaredThrowableException(e);
      }
    }
    return v;
  }

}

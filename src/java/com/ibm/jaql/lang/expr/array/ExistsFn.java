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
package com.ibm.jaql.lang.expr.array;

import static com.ibm.jaql.json.type.JsonType.ARRAY;
import static com.ibm.jaql.json.type.JsonType.NULL;

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

// TODO: redefine this for JSON; add empty()
/**
 * @jaqlDescription 
 * Usage : bool exists(any);
 * If the argument is null, return null ,
 * If the argument is a empty array , return false ,
 * If the argument is an array with at least one element, return true ,
 * If the argument is not an array or a null, return true. 
 * 
 * @jaqlExample exists(null);
 * null 
 * 
 * @jaqlExample exists([]);
 * false 
 * 
 * @jaqlExample exists([...]);
 * true //when the array has at least one element (even a null) 
 *      
 * @jaqlExample exists(...);
 * true //when the argument is not an array or a null
 */
public class ExistsFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("exists", ExistsFn.class);
    }
  }
  
  /**
   * exists(array)
   * 
   * @param exprs
   */
  public ExistsFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * exists(array)
   * 
   * @param expr
   */
  public ExistsFn(Expr expr)
  {
    super(expr);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  protected JsonValue evalRaw(final Context context) throws Exception
  {
    JsonIterator iter;
    Expr expr = exprs[0];
    if (expr.getSchema().is(ARRAY,NULL).always())
    {
      iter = expr.iter(context);
      if (iter.isNull())
      {
        return null;
      }
    }
    else
    {
      JsonValue w = expr.eval(context);
      if (w == null)
      {
        return null;
      }
      else if (w instanceof JsonArray)
      {
        iter = ((JsonArray) w).iter();
      }
      else
      {
        return JsonBool.TRUE;
      }
    }
    return JsonBool.make(iter.moveNext());
  }
}

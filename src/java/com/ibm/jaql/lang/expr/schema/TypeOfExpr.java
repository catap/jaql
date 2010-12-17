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
package com.ibm.jaql.lang.expr.schema;

import static com.ibm.jaql.json.type.JsonType.ARRAY;
import static com.ibm.jaql.json.type.JsonType.NULL;

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * 
 */
public class TypeOfExpr extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("typeof", TypeOfExpr.class);
    }
  }
  
  /**
   * @param exprs
   */
  public TypeOfExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  protected JsonString evalRaw(final Context context) throws Exception
  {
    Expr expr = exprs[0];
    if (expr.getSchema().is(ARRAY,NULL).always())
    {
      JsonIterator iter = expr.iter(context);
      if (iter.isNull())
      {
        return JsonType.NULL.getName();
      }
      else
      {
        return JsonType.ARRAY.getName();
      }
    }
    else
    {
      JsonValue value = expr.eval(context);
      if (value == null) {
        return JsonType.NULL.getName();
      } 
      else
      {
        return value.getType().getName();
      }      
    }
  }  
}

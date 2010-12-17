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

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * 
 */
public class DeemptyFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("deempty", DeemptyFn.class);
    }
  }
  
  /**
   * @param exprs
   */
  public DeemptyFn(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  protected JsonIterator iterRaw(final Context context) throws Exception
  {
    final JsonIterator iter = exprs[0].iter(context);
    if (iter.isNull())
    {
      return JsonIterator.NULL;
    }

    return new JsonIterator() {
      protected boolean moveNextRaw() throws Exception
      {
        while (true)
        {
          if (!iter.moveNext()) {
            return false;
          }
          JsonValue w = iter.current();
          if (w != null)
          {
            if (w instanceof JsonRecord)
            {
              if (((JsonRecord) w).size() > 0)
              {
                currentValue = w;
                return true;
              }
            }
            else if (w instanceof JsonArray)
            {
              if (!((JsonArray) w).isEmpty())
              {
                currentValue = w;
                return true;
              }
            }
            else
            {
              currentValue = w;
              return true;
            }
          }
        }
      }
    };
  }
}

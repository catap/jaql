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
package com.acme.extensions.expr;

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * 
 */
public class SplitIterExpr extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("splitArr", SplitIterExpr.class);
    }
  }
  
  /**
   * @param exprs
   */
  public SplitIterExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonIterator iter(Context context) throws Exception
  {
    // evaluate this expression's input.
    JsonValue sValue = exprs[0].eval(context);
    JsonValue dValue = exprs[1].eval(context);

    // if there is nothing to split or the split is not text, return nil
    if (sValue == null || !(sValue instanceof JsonString)) return JsonIterator.EMPTY;

    // if there is no delimter or the delimiter is not text, return nil
    if (dValue == null || !(dValue instanceof JsonString)) return JsonIterator.EMPTY;

    // get the input string  
    String s = ((JsonString) sValue).toString();

    // get the delimter
    String d = ((JsonString) dValue).toString();

    // split the string
    final String[] splits = s.split(d);

    return new JsonIterator() {
      int  i    = 0;
      int  n    = splits.length;

      @Override
      public boolean moveNext() throws Exception
      {
        if (i < n)
        {
          currentValue = new JsonString(splits[i]);
          i++;
          return true;
        }
        return false;
      }
    };
  }
}

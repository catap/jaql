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
package com.ibm.jaql.lang.expr.string;

import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.MutableJsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * @jaqlDescription Get a certain substring of a string, start from beginIdx , end to endIdx. If endIdx is not given or larger 
 * than the lenght of the string, return the substring from beginIdx to the end of the string.
 * 
 * Usage:
 * string substring(string val, int beginIdx, int endIndx ?);
 * 
 * @jaqlExample substring("I love the game", 2, 7);
 * "love"
 * 
 * @jaqlExample substring("I love the game", 2);
 * "love the game"
 * 
 * @jaqlExample substring("I love the game", 2, 20);
 * "love the game"
 */
public class SubstringFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par23
  {
    public Descriptor()
    {
      super("substring", SubstringFn.class);
    }
  }
  
  protected MutableJsonString result = new MutableJsonString();

  /**
   * @param exprs
   */
  public SubstringFn(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonString eval(final Context context) throws Exception
  {
    JsonString text = (JsonString) exprs[0].eval(context);
    if (text == null)
    {
      return null;
    }
    JsonNumber n = (JsonNumber) exprs[1].eval(context);
    if( n == null )
    {
      throw new RuntimeException("substring start index required");
    }
    int start = n.intValueExact();
    String s = text.toString(); // TODO: add JString.substring() methods with target buffer

    n = (JsonNumber) exprs[2].eval(context);
    if (n == null)
    {
      if( start == 0 )
      {
        return text;
      }
      s = s.substring(start); // TODO: switch to python/js semantics?
    }
    else
    {
      int end = n.intValueExact();
      if( end >= s.length() )
      {
        if( start == 0 )
        {
          return text;
        }
        end =  s.length();
      }
      s = s.substring(start, end); // TODO: switch to python/js semantics?
    }
    result.setCopy(s);
    return result;
  }
}

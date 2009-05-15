/*
 * Copyright (C) IBM Corp. 2009.
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

import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonNumeric;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.Bool3;


/**
 * splitAt(string src, string sep, int n) ==> [string1, string2, ..., stringn]
 * sep is a string of one charater.
 */
@JaqlFn(fnName = "strSplitN", minArgs = 3, maxArgs = 3)
public class StrSplitNFn extends Expr
{
  protected BufferedJsonArray tuple = new BufferedJsonArray();
  protected JsonString[] resultStrings = new JsonString[0];
  
  /**
   * @param args
   */
  public StrSplitNFn(Expr[] args)
  {
    super(args);
  }

  @Override
  public Bool3 isArray()
  {
    return Bool3.TRUE;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonArray eval(final Context context) throws Exception
  {
    // TODO: need a way to avoid recomputing const exprs...
    JsonString sep = JaqlUtil.enforceNonNull((JsonString)exprs[1].eval(context));
    JsonNumeric num = JaqlUtil.enforceNonNull((JsonNumeric)exprs[2].eval(context));
    char c = sep.toString().charAt(0);
    int n = num.intValueExact();
    
    JsonString str = (JsonString)exprs[0].eval(context);
    if( str == null )
    {
      return null;
    }
    
    // TODO: would be nice to not convert utf8 and string so much...
    String s = str.toString();
    if( n > resultStrings.length )
    {
      tuple.resize(n);
      JsonString[] rs = new JsonString[n];
      System.arraycopy(resultStrings, 0, rs, 0, resultStrings.length);
      for(int i = resultStrings.length ; i < n ; i++ )
      {
        rs[i] = new JsonString();
        tuple.set(i, rs[i]);
      }
      resultStrings = rs;
    }
    tuple.resize(n);
    
    int p = 0;
    String ss;
    for(int i = 0 ; i < n - 1 ; i++)
    {
      ss = "";
      if( p >= 0 )
      {
        int q = s.indexOf(c,p);
        if( q >= 0 )
        {
          ss = s.substring(p,q);
        }
        p = q + 1;
      }
      resultStrings[i].set(ss);
    }
    ss = (p >= 0) ? s.substring(p) : ""; 
    resultStrings[n-1].set(ss);
    return tuple;
  }
}

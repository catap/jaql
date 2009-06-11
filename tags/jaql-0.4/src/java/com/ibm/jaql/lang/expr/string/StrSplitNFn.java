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

import com.ibm.jaql.json.type.FixedJArray;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JNumeric;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.util.Bool3;


/**
 * splitAt(string src, string sep, int n) ==> [string1, string2, ..., stringn]
 * sep is a string of one charater.
 */
@JaqlFn(fnName = "strSplitN", minArgs = 3, maxArgs = 3)
public class StrSplitNFn extends Expr
{
  protected FixedJArray tuple = new FixedJArray();
  protected Item result = new Item(tuple);
  protected JString[] resultStrings = new JString[0];
  
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
  public Item eval(final Context context) throws Exception
  {
    // TODO: need a way to avoid recomputing const exprs...
    JString sep = (JString)exprs[1].eval(context).getNonNull();
    JNumeric num = (JNumeric)exprs[2].eval(context).getNonNull();
    char c = sep.toString().charAt(0);
    int n = num.intValueExact();
    
    JString str = (JString)exprs[0].eval(context).get();
    if( str == null )
    {
      return Item.NIL;
    }
    
    // TODO: would be nice to not convert utf8 and string so much...
    String s = str.toString();
    if( n > resultStrings.length )
    {
      tuple.resize(n);
      JString[] rs = new JString[n];
      System.arraycopy(resultStrings, 0, rs, 0, resultStrings.length);
      for(int i = resultStrings.length ; i < n ; i++ )
      {
        rs[i] = new JString();
        tuple.set(i, new Item(rs[i]));
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
    return result;
  }
}

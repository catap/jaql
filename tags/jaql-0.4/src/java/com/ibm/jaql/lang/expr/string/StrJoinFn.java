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

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;


/**
 * string strJoin(array items, string sep) 
 * Build a string that concatentates all the items, adding sep between each item.
 * Nulls are removed, without any separator.
 * If you want nulls, use firstNonNull(e,'how nulls appear').
 * 
 */
@JaqlFn(fnName = "strJoin", minArgs = 2, maxArgs = 2)
public class StrJoinFn extends Expr // TODO: make Aggregate?
{
  protected StringBuilder builder;
  protected JString text;
  protected Item result;
  
  /**
   * @param exprs
   */
  public StrJoinFn(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(Context context) throws Exception
  {
    if( result == null )
    {
      builder = new StringBuilder();
      text = new JString();
      result = new Item(text);
    }
    else
    {
      builder.setLength(0);
    }
    JValue v = exprs[1].eval(context).get();
    String theSep = ( v == null ) ? "" : v.toString();
    String sep = "";
    Iter iter = exprs[0].iter(context);
    Item item;
    while( (item = iter.next()) != null )
    {
      v = item.get();
      if( v != null ) 
      {
        builder.append(sep);
        String s = v.toString(); // TODO: add toJString() ?
        builder.append(s);
        sep = theSep;
      }
    }
    text.set(builder.toString());
    return result;
  }
}

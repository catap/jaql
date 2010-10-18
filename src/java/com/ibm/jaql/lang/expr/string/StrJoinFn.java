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

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonString;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;


/**
 * string strJoin(array items, string sep) 
 * Build a string that concatentates all the items, adding sep between each item.
 * Nulls are removed, without any separator.
 * If you want nulls, use firstNonNull(e,'how nulls appear').
 * 
 */
public class StrJoinFn extends Expr // TODO: make Aggregate?
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("strJoin", StrJoinFn.class);
    }
  }
  
  protected StringBuilder builder;
  protected MutableJsonString text;
  
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
  public JsonString eval(Context context) throws Exception
  {
    if( text == null )
    {
      builder = new StringBuilder();
      text = new MutableJsonString();      
    }
    else
    {
      builder.setLength(0);
    }
    JsonValue value = exprs[1].eval(context);
    if( !(value instanceof JsonString) )
    	throw new Exception("Separator for strJoin must be of type JsonString, received: " + value.getType() );
    String theSep = ( value == null ) ? "" : value.toString();
    String sep = "";
    JsonIterator iter = exprs[0].iter(context);
    for (JsonValue v : iter)
    {
      if( v != null ) 
      {
        builder.append(sep);
        String s = v.toString(); // TODO: add toJString() ?
        builder.append(s);
        sep = theSep;
      }
    }
    text.setCopy(builder.toString());
    return text;
  }
}

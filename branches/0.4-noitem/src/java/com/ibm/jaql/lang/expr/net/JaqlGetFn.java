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
package com.ibm.jaql.lang.expr.net;

import java.io.InputStream;
import java.net.URL;
import java.net.URLEncoder;

import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "jaqlGet", minArgs = 1, maxArgs = 2)
public class JaqlGetFn extends Expr
{
  /**
   * @param exprs
   */
  public JaqlGetFn(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isConst()
   */
  @Override
  public boolean isConst()
  {
    return false;
  }

  /**
   * @param context
   * @return
   * @throws Exception
   */
  protected JsonParser fetch(Context context) throws Exception
  {
    JsonString urlText = (JsonString) exprs[0].eval(context);
    String urlStr = urlText.toString();
    // TODO: memory!!
    if (exprs.length == 2)
    {
      JsonRecord args = (JsonRecord) exprs[1].eval(context);
      if (args != null)
      {
        String sep = "?";
        for (int i = 0; i < args.arity(); i++)
        {
          JsonString name = args.getName(i);          
          JsonValue w = args.getValue(i);
          if (w != null)
          {
            String s = w.toString();
            // System.out.println(name + "=" + s);
            s = URLEncoder.encode(s, "UTF-8");
            urlStr += sep + name + "=" + s;
            sep = "&";
          }
        }
      }
    }
    URL url = new URL(urlStr);
    InputStream in = url.openStream();
    JsonParser parser = new JsonParser(in);
    return parser;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonValue eval(Context context) throws Exception
  {
    JsonParser parser = fetch(context);
    JsonValue value = parser.TopVal();
    return value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(Context context) throws Exception
  {
    final JsonParser parser = fetch(context);
    return new JsonIterator() {
      JsonValue nextValue = parser.ArrayFirst();
      boolean checkedEOF = false;

      @Override
      public boolean moveNext() throws Exception
      {
        if (nextValue == JsonParser.NIL)
        {
          if (!checkedEOF)
          {
            parser.Eof();
            checkedEOF = true;
          }
          return false;
        }
        currentValue = nextValue;
        nextValue = parser.ArrayNext();
        return true;
      }
    };
  }
}

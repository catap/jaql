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
package com.ibm.jaql.lang.expr.core;

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonDate;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;

@JaqlFn(fnName="perf", minArgs=1, maxArgs=1)
public class PerfFn extends Expr
{
  public PerfFn(Expr[] inputs)
  {
    super(inputs);
  }
  
  @Override
  public JsonValue eval(final Context context) throws Exception
  {
    Expr e = exprs[0];
    long start = System.currentTimeMillis();
    long n = 0;
    if( e.getSchema().isArrayOrNull().always() )
    {
      JsonIterator iter = e.iter(context);
      while (iter.moveNext()) 
      {
        n++;
      }
    }
    else
    {
      JsonValue val = e.eval(context);
      if( val instanceof JsonArray )
      {
        n = ((JsonArray)val).count();
      }
      else
      {
        n = 1;
      }
    }
    long end = System.currentTimeMillis();
    BufferedJsonRecord rec = new BufferedJsonRecord();
    rec.add(new JsonString("start"), new JsonDate(start));
    rec.add(new JsonString("end"), new JsonDate(end));
    rec.add(new JsonString("millis"), new JsonLong(end - start));
    rec.add(new JsonString("count"), new JsonLong(n));
    return rec;
  }
}

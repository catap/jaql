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
package com.ibm.jaql.lang.expr.index;

import com.ibm.jaql.io.index.JIndexWriter;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;


@JaqlFn(fnName = "buildJIndex", minArgs = 2, maxArgs = 2)
public class BuildJIndexFn extends Expr
{
  public BuildJIndexFn(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public JsonValue eval(Context context) throws Exception
  {
    JsonRecord fd = (JsonRecord)exprs[1].eval(context);
    if( fd == null )
    {
      return null;
    }
    JsonString jloc = (JsonString)fd.getRequired(new JsonString("location"));
    if( jloc == null )
    {
      return null;
    }

    JIndexWriter index = new JIndexWriter(jloc.toString());

    JsonValue[] kvpair = new JsonValue[2];

    JsonIterator iter = exprs[0].iter(context);
    for (JsonValue value : iter)
    {
      JsonArray arr = (JsonArray)value;
      arr.getAll(kvpair);
      JsonValue key = kvpair[0];
      JsonValue val = kvpair[1];
      index.add(key,val);
    }
    
    index.close();
    
    return fd;
  }
}

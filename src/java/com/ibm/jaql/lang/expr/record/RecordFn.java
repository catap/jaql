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
package com.ibm.jaql.lang.expr.record;

import java.util.Map.Entry;

import com.ibm.jaql.json.type.BufferedJsonRecord;
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
@JaqlFn(fnName = "record", minArgs = 1, maxArgs = 1)
public class RecordFn extends Expr // TODO: make into an aggregate?
{
  protected BufferedJsonRecord rec;

  /**
   * @param exprs
   */
  public RecordFn(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonRecord eval(final Context context) throws Exception
  {
    if (rec == null)
    {
      rec = new BufferedJsonRecord();
    }
    else
    {
      rec.clear();
    }
    
    JsonIterator iter = exprs[0].iter(context);
    for (JsonValue v : iter) 
    {
      JsonRecord inrec = (JsonRecord)v;
      if (inrec != null)
      {
        rec.ensureCapacity(rec.size() + inrec.size());
        for (Entry<JsonString, JsonValue> e : inrec)
        {
          JsonString name = rec.getName(rec.size()); // reuse
          JsonValue value = rec.get(rec.size()); // reuse
          name = e.getKey().getCopy(name);
          value = e.getValue().getCopy(value);
          rec.add(name, value);
        }
      }
    }
    return rec;
  }
}

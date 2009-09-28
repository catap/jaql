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
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * 
 */
public class RemapFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("remap", RemapFn.class);
    }
  }
  
  /**
   * removeFields(oldRec, newRec)
   * 
   * @param exprs
   */
  public RemapFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param oldRec
   * @param newRec
   */
  public RemapFn(Expr oldRec, Expr newRec)
  {
    super(new Expr[]{oldRec, newRec});
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonRecord eval(Context context) throws Exception
  {
    JsonRecord oldRec = (JsonRecord) exprs[0].eval(context);
    JsonRecord newRec = (JsonRecord) exprs[1].eval(context);
    if (oldRec == null)
    {
      return newRec;
    }
    if (newRec == null)
    {
      return oldRec;
    }

    BufferedJsonRecord outRec = new BufferedJsonRecord(); // TODO: memory

    for (Entry<JsonString, JsonValue> e : newRec)
    {
      outRec.add(e.getKey(), e.getValue());
    }

    for (Entry<JsonString, JsonValue> e : oldRec)
    {
      JsonString nm = e.getKey();
      if (newRec.get(nm, null) == null) // TODO: null value overwrite intended?
      {
        outRec.add(nm, e.getValue());
      }
    }

    return outRec;
  }
}

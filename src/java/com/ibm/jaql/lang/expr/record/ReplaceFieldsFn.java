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
 * @jaqlDescription Replace fields in oldRec with fields in newRec only if the field name exists in oldRec.
 * Unlike remap, this only replaces existing fields.
 * 
 * @jaqlExample
 *
 */
public class ReplaceFieldsFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("replaceFields", ReplaceFieldsFn.class);
    }
  }
  
  /**
   * replaceFields(oldRec, newRec)
   * 
   * @param exprs
   */
  public ReplaceFieldsFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param oldRec
   * @param newRec
   */
  public ReplaceFieldsFn(Expr oldRec, Expr newRec)
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
    JsonRecord oldRec = (JsonRecord)exprs[0].eval(context);
    JsonRecord newRec = (JsonRecord)exprs[1].eval(context);
    if (oldRec == null)
    {
      return null;
    }
    if (newRec == null)
    {
      return oldRec;
    }

    BufferedJsonRecord outRec = new BufferedJsonRecord(); // TODO: memory

    for (Entry<JsonString, JsonValue> e : oldRec)
    {
      JsonString nm = e.getKey();
      JsonValue value = newRec.get(nm);
      if( value == null )
      {
        value = e.getValue();
      }
      outRec.add(nm, value);
    }
    
    return outRec;
  }
}

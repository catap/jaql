/*
 * Copyright (C) IBM Corp. 2010.
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
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;

/**
 * A special case of record construction where every field is known (constant) and required.
 * For example: { x: e1, y: e2 }
 * But not:     { x?: e1, y: e2 }
 *      or:     { (...): e1, y: e2 } 
 *      or:     { e1.* }
 *      or:     { e1.x }   if x is not known to exist
 */
public class FixedRecordExpr extends RecordExpr
{
  protected JsonString[] names;
  protected JsonValue[] values;
  
  public FixedRecordExpr(Expr... exprs)
  {
    super(exprs);
  }
  
  @Override
  public JsonValue eval(Context context) throws Exception
  {
    if (record == null)
    {
      names = new JsonString[exprs.length];
      values = new JsonValue[exprs.length];
      boolean sorted = true;
      for (int i = 0; i < exprs.length; i++)
      {
        Expr e = exprs[i];
        if( e instanceof NameValueBinding )
        {
          NameValueBinding f = (NameValueBinding)e;
          assert f.isRequired() && f.nameExpr() instanceof ConstExpr;
          names[i] = (JsonString) f.nameExpr().eval(context);
        }
        else if( e instanceof CopyField )
        {
          CopyField f = (CopyField)e;
          assert f.when == CopyField.When.ALWAYS && f.nameExpr() instanceof ConstExpr;
          names[i] = (JsonString) f.nameExpr().eval(context);
        }
        else
        {
          throw new IllegalStateException("FixedRecord does not support " + e.getClass().getName());
        }
        sorted = sorted && (i == 0 || names[i-1].compareTo(names[i]) < 0);
      }
      record = new BufferedJsonRecord();
      record.set(names, values, exprs.length, sorted);
    }

    for (int i = 0; i < exprs.length; i++)
    {
      Expr e = exprs[i];
      // TODO: provide a Field API for evalValue
      if( e instanceof NameValueBinding )
      {
        NameValueBinding f = (NameValueBinding)e;
        values[i] = f.valueExpr().eval(context);
      }
      else
      {
        CopyField f = (CopyField)e;
        JsonRecord rec = (JsonRecord)f.recExpr().eval(context);
        // rec should never be null if we are using a FixedRecordExpr
        values[i] = rec.get(names[i]);
      }
    }
    return record;
  }
}

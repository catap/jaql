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

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.MemoryJRecord;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * Replace fields in oldRec with fields in newRec only if the field name exists in oldRec.
 * Unlike remap, this only replaces existing fields.
 */
@JaqlFn(fnName = "replaceFields", minArgs = 2, maxArgs = 2)
public class ReplaceFieldsFn extends Expr
{
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
  public Item eval(Context context) throws Exception
  {
    Item oldItem = exprs[0].eval(context);
    Item newItem = exprs[1].eval(context);
    JRecord oldRec = (JRecord) oldItem.get();
    JRecord newRec = (JRecord) newItem.get();
    if (oldRec == null)
    {
      return Item.NIL;
    }
    if (newRec == null)
    {
      return oldItem;
    }

    MemoryJRecord outRec = new MemoryJRecord(); // TODO: memory
    Item result = new Item(outRec); // TODO: memory

    int n = oldRec.arity();
    for (int i = 0; i < n; i++)
    {
      JString nm = oldRec.getName(i);
      Item value = newRec.getValue(nm, null);
      if( value == null )
      {
        value = oldRec.getValue(i);
      }
      outRec.add(nm, value);
    }
    
    return result;
  }
}

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

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.MemoryJRecord;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "record", minArgs = 1, maxArgs = 1)
public class RecordFn extends Expr // TODO: make into an aggregate?
{
  protected MemoryJRecord rec;
  protected Item resultRec;

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
  public Item eval(final Context context) throws Exception
  {
    if (rec == null)
    {
      rec = new MemoryJRecord();
      resultRec = new Item(rec);
    }
    else
    {
      rec.clear();
    }
    Item item;
    Iter iter = exprs[0].iter(context);
    while ((item = iter.next()) != null)
    {
      JRecord inrec = (JRecord) item.get();
      if (inrec != null)
      {
        int n = inrec.arity();
        rec.ensureCapacity(rec.getCapacity() + n);
        for (int i = 0; i < n; i++)
        {
          JString name = rec.getName(rec.arity());
          Item value = rec.getValue(rec.arity());
          name.setCopy(inrec.getName(i));
          value.copy(inrec.getValue(i));
          rec.add(name, value);
        }
      }
    }
    return resultRec;
  }
}

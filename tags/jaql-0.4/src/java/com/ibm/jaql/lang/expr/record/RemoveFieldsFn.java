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

import java.util.HashSet;

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
@JaqlFn(fnName = "removeFields", minArgs = 2, maxArgs = 2)
public class RemoveFieldsFn extends Expr
{
  /**
   * removeFields(rec, [names])
   * 
   * @param exprs
   */
  public RemoveFieldsFn(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(Context context) throws Exception
  {
    JRecord rec = (JRecord) exprs[0].eval(context).get();
    if (rec == null)
    {
      return Item.NIL;
    }

    Iter iter = exprs[1].iter(context);
    // TODO: it would be great to detect a constant list of names here
    HashSet<JString> removeNames = new HashSet<JString>(); // TODO: memory
    Item name;
    while ((name = iter.next()) != null)
    {
      JString nm = (JString) name.get();
      removeNames.add(new JString(nm)); // TODO: memory
    }

    MemoryJRecord outRec = new MemoryJRecord(); // TODO: memory
    Item result = new Item(outRec); // TODO: memory

    int n = rec.arity();
    for (int i = 0; i < n; i++)
    {
      JString nm = rec.getName(i);
      if (!removeNames.contains(nm))
      {
        outRec.add(nm, rec.getValue(i));
      }
    }

    return result;
  }
}

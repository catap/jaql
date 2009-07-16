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
package com.ibm.jaql.lang.expr.array;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.MemoryJRecord;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.util.Bool3;


/**
 * 
 */
@JaqlFn(fnName = "arrayToRecord", minArgs = 2, maxArgs = 2)
public class ArrayToRecordFn extends Expr
{
  /**
   * @param args: array names, array values
   */
  public ArrayToRecordFn(Expr[] args)
  {
    super(args);
  }

  @Override
  public Bool3 isArray()
  {
    return Bool3.FALSE;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(final Context context) throws Exception
  {
    final Iter names  = exprs[0].iter(context);
    final Iter values = exprs[1].iter(context);
    Item n,v;
    MemoryJRecord rec = new MemoryJRecord(); // TODO: memory
    Item result = new Item(rec); // TODO: memory
    while( (n = names.next()) != null && 
           (v = values.next()) != null )
    {
      JString s = (JString)n.get();
      rec.add(s,v);
    }
    return result;
  }
}

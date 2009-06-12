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

import com.ibm.jaql.json.type.FixedJArray;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.util.Bool3;

/**
 * 
 */
@JaqlFn(fnName = "fields", minArgs = 1, maxArgs = 1)
public final class FieldsFn extends IterExpr
{
  /**
   * Expr record
   * 
   * @param exprs
   */
  public FieldsFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param recExpr
   */
  public FieldsFn(Expr recExpr)
  {
    this(new Expr[]{recExpr});
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isNull()
   */
  @Override
  public Bool3 isNull()
  {
    return exprs[0].isNull();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public Iter iter(final Context context) throws Exception
  {
    final JRecord rec = (JRecord) exprs[0].eval(context).get();
    if (rec == null)
    {
      return Iter.nil; // TODO: should this be []?
    }
    final FixedJArray pair = new FixedJArray(2);
    final Item nameItem = new Item();
    pair.set(0, nameItem);
    final Item result = new Item(pair);

    return new Iter() {
      int slot = 0;

      public Item next() throws Exception
      {
        if (slot >= rec.arity())
        {
          return null;
        }
        nameItem.set(rec.getName(slot));
        pair.set(1, rec.getValue(slot));
        slot++;
        return result;
      }
    };
  }

}

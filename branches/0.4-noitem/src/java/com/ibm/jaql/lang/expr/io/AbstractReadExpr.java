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
package com.ibm.jaql.lang.expr.io;

import com.ibm.jaql.io.InputAdapter;
import com.ibm.jaql.io.ItemReader;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.Bool3;

public abstract class AbstractReadExpr extends IterExpr
{

  public AbstractReadExpr(Expr[] exprs)
  {
    super(exprs);
  }

  public AbstractReadExpr(Expr fd)
  {
    super(fd);
  }

  /**
   * @return
   */
  public final Expr descriptor()
  {
    return exprs[0];
  }

  @Override
  public boolean isConst()
  {
    return false;
  }

  @Override
  public Bool3 isNull()
  {
    return Bool3.FALSE;
  }

  @Override
  public Iter iter(Context context) throws Exception
  {
    // evaluate the arguments
    Item args = exprs[0].eval(context);
  
    // get the InputAdapter according to the type
    final InputAdapter adapter = (InputAdapter) JaqlUtil.getAdapterStore().input.getAdapter(args);
    adapter.open();
    return new Iter() {
      ItemReader reader = adapter.getItemReader();
      Item       item   = reader.createValue();
  
      @Override
      public Item next() throws Exception
      {
        while (true)
        {
          if (reader == null) return null;
          if (reader.next(item))
          {
            return item;
          }
          reader.close();
          reader = null;
        }
      }
    };
  }
}

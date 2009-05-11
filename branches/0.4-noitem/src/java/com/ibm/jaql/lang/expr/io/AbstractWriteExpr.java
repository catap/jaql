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

import com.ibm.jaql.io.ItemWriter;
import com.ibm.jaql.io.OutputAdapter;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.util.JaqlUtil;

public abstract class AbstractWriteExpr extends Expr
{
  public AbstractWriteExpr(Expr[] exprs)
  {
    super(exprs);
  }

  public AbstractWriteExpr(Expr toWrite, Expr fd)
  {
    super(toWrite, fd);
  }

  /**
   * @return
   */
  public final Expr dataExpr()
  {
    return exprs[0];
  }

  /**
   * @return
   */
  public final Expr descriptor()
  {
    return exprs[1];
  }

  @Override
  public boolean isConst()
  {
    return false;
  }

  @Override
  public Item eval(Context context) throws Exception
  {
    //  evaluate the arguments
    Item argsItem = descriptor().eval(context);
    
    // get the OutputAdapter according to the type
    OutputAdapter adapter = (OutputAdapter) JaqlUtil.getAdapterStore().output.getAdapter(argsItem);
  
    adapter.open();
    ItemWriter writer = adapter.getItemWriter();
    Iter iter = dataExpr().iter(context);
    if (iter != null)
    {
      Item item;
      //Item key = Item.nil; // the key is not used
      while ((item = iter.next()) != null)
      {
        writer.write(item);
      }
    }
    adapter.close();
  
    return argsItem;
  }
}

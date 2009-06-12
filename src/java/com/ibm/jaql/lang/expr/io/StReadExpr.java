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
package com.ibm.jaql.lang.expr.io;

import com.ibm.jaql.io.InputAdapter;
import com.ibm.jaql.io.ItemReader;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.lang.expr.core.RecordExpr;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.Bool3;

/**
 * An expression used for reading data into jaql. It is called as follows:
 * read({type: '...', location: '...', inoptions: {...}}) <br>
 * The type specifies which InputAdapter to use, the location specifies the
 * address from which the adapter will read. The optional inoptions further
 * parametrize the adapter's behavior. <br>
 * If inoptions are not specified, then default options that are registered for
 * the type at the AdapterStore will be used. If no options are specified and
 * there are no defaults registered, it is an error. If both options are
 * specified and default options are registered, then the union of option fields
 * will be used. If there are duplicate names, then the query options will be
 * used as an override.
 */
@JaqlFn(fnName = "stRead", minArgs = 1, maxArgs = 1)
public final class StReadExpr extends IterExpr implements PotentialMapReducible
{
  /**
   * @param exprs
   */
  public StReadExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param e
   */
  public StReadExpr(Expr e)
  {
    super(new Expr[]{e});
  }

  /**
   * @return
   */
  public Expr descriptor()
  {
    return exprs[0];
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isConst()
   */
  @Override
  public boolean isConst()
  {
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isNull()
   */
  @Override
  public Bool3 isNull()
  {
    return Bool3.FALSE;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.PotentialMapReducible#isMapReducible()
   */
  public boolean isMapReducible()
  {
    return MapReducibleUtil.isMapReducible(true, exprs[0]);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.PotentialMapReducible#rewriteToMapReduce(com.ibm.jaql.lang.Expr)
   */
  public Expr rewriteToMapReduce(Expr expr)
  {
    if (exprs[0] instanceof RecordExpr && expr instanceof RecordExpr)
      return MapReducibleUtil.rewriteToMapReduce((RecordExpr) exprs[0],
          (RecordExpr) expr);
    return exprs[0];
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.IterExpr#iter(com.ibm.jaql.lang.Context)
   */
  @Override
  public Iter iter(Context context) throws Exception
  {
    // evaluate the arguments
    Item args = exprs[0].eval(context);

    // get the InputAdapter according to the type
    final InputAdapter adapter = (InputAdapter) JaqlUtil.getAdapterStore().input
        .getAdapter(args);
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

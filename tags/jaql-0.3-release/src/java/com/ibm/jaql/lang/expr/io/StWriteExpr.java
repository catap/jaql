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

import com.ibm.jaql.io.ItemWriter;
import com.ibm.jaql.io.OutputAdapter;
import com.ibm.jaql.io.hadoop.HadoopOutputAdapter;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.lang.expr.core.RecordExpr;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * An expression used for writing external data. It is called as follows:
 * write({type: '...', location: '...', outoptions: '...', inoptions: '...'},
 * expr); <br>
 * The type specifies which OutputAdapter to use, the location specifies the
 * address to which the adapter will write. The optional outoptions further
 * parametrize the adapter's behavior. The optional inoptions can be used to
 * parametrize a read expression that takes as input a write expression (e.g.,
 * read(write({...}, expr)) ). <br>
 * If outoptions or inoptions are not specified, then default options that are
 * registered for the type at the AdapterStore will be used. If no options are
 * specified and there are no defaults registered, it is an error. If both
 * options are specified and default options are registered, then the union of
 * option fields will be used. If there are duplicate names, then the query
 * options will be used as an override.
 */
@JaqlFn(fnName = "stWrite", minArgs = 2, maxArgs = 2)
public final class StWriteExpr extends Expr implements PotentialMapReducible
{
  /**
   * @param exprs
   */
  public StWriteExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param fd
   * @param toWrite
   */
  public StWriteExpr(Expr fd, Expr toWrite)
  {
    super(new Expr[]{fd, toWrite});
  }

  /**
   * @return
   */
  public final Expr descriptor()
  {
    return exprs[0];
  }

  /**
   * @return
   */
  public final Expr dataExpr()
  {
    return exprs[1];
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
   * @see com.ibm.jaql.lang.Expr#eval(com.ibm.jaql.lang.Context)
   */
  @Override
  public Item eval(Context context) throws Exception
  {
    //  evaluate the arguments
    Item argsItem = exprs[0].eval(context);
    JRecord args = (JRecord) argsItem.get();

    // get the OutputAdapter according to the type
    OutputAdapter adapter = (OutputAdapter) JaqlUtil.getAdapterStore().output
        .getAdapter(argsItem);

    adapter.open();
    ItemWriter writer = adapter.getItemWriter();
    Iter iter = exprs[1].iter(context);
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

  // FIXME: get rid of this...
  protected void setupOutputFormatExpr(HadoopOutputAdapter adapter, JRecord args)
      throws Exception
  {

  }
}

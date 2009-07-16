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

import com.ibm.jaql.io.hbase.HBaseStore;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JBool;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * hbaseDelete(tableExpr, keyExpr, columnExpr)
 * 
 * string tableName <- eval tableExpr JArray keyValue <- eval keyExpr JArray of
 * Texts <- eval columnExpr
 * 
 * Delete all column values for the given tuple identified by the keyExpr in the
 * table defined by tableExpr.
 */
@JaqlFn(fnName = "hbaseDelete", minArgs = 3, maxArgs = 3)
public class HBaseDeleteExpr extends Expr
{
  /**
   * hbaseDelete( string table, array keys, array columns )
   * 
   * @param exprs
   */
  public HBaseDeleteExpr(Expr[] exprs)
  {
    super(exprs);
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
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(final Context context) throws Exception
  {
    // get the table name
    JString tableName = (JString) exprs[0].eval(context).getNonNull();

    JArray jcolumns = (JArray) exprs[2].eval(context).getNonNull();

    // For each key, delete all the columns specified (or all columns)
    // TODO: return the number of matching keys?  the keys plus an indicator?
    Iter rows = exprs[1].iter(context);

    // do the deletes
    HBaseStore.Util.deleteValues(tableName, jcolumns, rows);

    return JBool.trueItem;
  }
}

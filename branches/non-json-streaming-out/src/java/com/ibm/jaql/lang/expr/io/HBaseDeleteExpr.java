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

import java.util.Map;

import com.ibm.jaql.io.hbase.HBaseStore;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.lang.util.JaqlUtil;

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

  public Map<ExprProperty, Boolean> getProperties()
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    result.put(ExprProperty.HAS_SIDE_EFFECTS, true);
    return result;
  }


  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonBool eval(final Context context) throws Exception
  {
    // get the table name
    JsonString tableName = JaqlUtil.enforceNonNull((JsonString) exprs[0].eval(context));

    JsonArray jcolumns = JaqlUtil.enforceNonNull((JsonArray) exprs[2].eval(context));

    // For each key, delete all the columns specified (or all columns)
    // TODO: return the number of matching keys?  the keys plus an indicator?
    JsonIterator rows = exprs[1].iter(context);

    // do the deletes
    HBaseStore.Util.deleteValues(tableName, jcolumns, rows);

    return JsonBool.TRUE;
  }
}

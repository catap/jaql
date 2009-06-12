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
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * hbaseFetch( tableExpr, keyExpr, |columnExpr, {timestamp:timestampExpr,
 * numversions:numversionsExpr}|)
 *  - Text tableName <- evaluate tableExpr - JArray keysExpr <- evaluate keyExpr -
 * JArray columnExpr <- evaluate columnExpr - LongItem timestamp <- evaluate
 * timestamp - LongItem numVersions <- evaluate numversions
 * 
 * If only keyExpr is specified, a tuple is returned. If keyExpr and numversions
 * are specified, then multiple values may be returned for a column. In this
 * case, the result that is returned is as follows: {key: value, column: [v1,
 * v2, ...], column: [...]}
 */
@JaqlFn(fnName = "hbaseFetch", minArgs = 2, maxArgs = 4)
public class HBaseFetchExpr extends IterExpr
{
  /**
   * hbaseFetch( tableExpr, keysExpr, (, columnsExpr)? (, argsExpr)? )
   * 
   * @param exprs
   */
  public HBaseFetchExpr(Expr[] exprs)
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
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public Iter iter(final Context context) throws Exception
  {
    //  get the table name
    JString tableName = (JString) exprs[0].eval(context).getNonNull();

    JArray jcolumns = null;
    JRecord args = JRecord.empty;
    if (exprs.length == 3)
    {
      Item tmp = exprs[2].eval(context);
      JValue w = tmp.get();
      if (w != null)
      {
        if (w instanceof JRecord)
        {
          args = (JRecord) w;
        }
        else if (w instanceof JArray)
        {
          jcolumns = (JArray) w;
        }
        else
        {
          throw new Exception(
              "Expected either columns (array) or optional arguments (record), received: "
                  + w);
        }
      }
    }
    else if (exprs.length == 4)
    {
      jcolumns = (JArray) exprs[2].eval(context).get();
      args = (JRecord) exprs[3].eval(context).get();
    }
    // get the arguments
    JLong timestampValue = (JLong) args.getValue("timestamp").get();
    JLong numVersionsValue = (JLong) args.getValue("numversions").get();

    // the iterator for record keys
    Iter rows = exprs[1].iter(context);

    // fetch the record(s)
    return HBaseStore.Util.fetchRecords(tableName, jcolumns, timestampValue,
        numVersionsValue, rows);
  }
}

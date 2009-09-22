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
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.lang.util.JaqlUtil;

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

  public Map<ExprProperty, Boolean> getProperties()
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    result.put(ExprProperty.HAS_SIDE_EFFECTS, true);
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(final Context context) throws Exception
  {
    //  get the table name
    JsonString tableName = JaqlUtil.enforceNonNull((JsonString) exprs[0].eval(context));

    JsonArray jcolumns = null;
    JsonRecord args = JsonRecord.EMPTY;
    if (exprs.length == 3)
    {
      JsonValue w = exprs[2].eval(context);
      if (w != null)
      {
        if (w instanceof JsonRecord)
        {
          args = (JsonRecord) w;
        }
        else if (w instanceof JsonArray)
        {
          jcolumns = (JsonArray) w;
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
      jcolumns = (JsonArray) exprs[2].eval(context);
      args = (JsonRecord) exprs[3].eval(context);
    }
    // get the arguments
    JsonLong timestampValue = (JsonLong) args.get(new JsonString("timestamp"));
    JsonLong numVersionsValue = (JsonLong) args.get(new JsonString("numversions"));

    // the iterator for record keys
    JsonIterator rows = exprs[1].iter(context);

    // fetch the record(s)
    return HBaseStore.Util.fetchRecords(tableName, jcolumns, timestampValue,
        numVersionsValue, rows);
  }
}

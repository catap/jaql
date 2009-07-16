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

import java.util.Map;

import com.ibm.jaql.io.ClosableJsonIterator;
import com.ibm.jaql.io.InputAdapter;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
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

  public Map<ExprProperty, Boolean> getProperties()
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    result.put(ExprProperty.READS_EXTERNAL_DATA, true);
    return result;
  }
  
  @Override
  public Schema getSchema()
  {
    // TODO: this is a quick hack to derive schema
    try
    {
      if (exprs[0].isCompileTimeComputable().always())
      {
        JsonValue args = exprs[0].eval(Env.getCompileTimeContext()); // TODO should provide context
        InputAdapter adapter = (InputAdapter) JaqlUtil.getAdapterStore().input.getAdapter(args);
        Schema s = adapter.getSchema();
        assert s.isArrayOrNull().always();
        return s;
      }
    } catch (Exception e)
    {
      // ignore
    }
    return SchemaFactory.arraySchema();
  }

  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }


  @Override
  public JsonIterator iter(Context context) throws Exception
  {
    // evaluate the arguments
    JsonValue args = exprs[0].eval(context);
  
    // get the InputAdapter according to the type
    final InputAdapter adapter = (InputAdapter) JaqlUtil.getAdapterStore().input.getAdapter(args);
    adapter.open();
    return new JsonIterator() {
      ClosableJsonIterator reader = adapter.iter();
  
      @Override
      public boolean moveNext() throws Exception
      {
        if (reader.moveNext()) 
        {
          currentValue = reader.current();
          return true;
        }
        else
        {
          reader.close();
          reader = null;
          return false;
        }
      }
    };
  }
}

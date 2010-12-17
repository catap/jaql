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

import static com.ibm.jaql.json.type.JsonType.NULL;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * @jaqlDescription Convert each key-value pair of a record to a [key,value] array.
 * 
 * Usage:
 * array fields(record r)
 * 
 * @jaqlExample fields({a:1, b:2, c:3});
 * [ ["a",1] , ["b",2] , ["c",3] ]
 * 
 * @jaqlExample fields({a:1, b:2, c:3}) -> transform $[0];
 * [ "a","b","c" ] //this example indicates a way to extract all the key values in a record.
 */
public final class FieldsFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("fields", FieldsFn.class);
    }
  }
  
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

  public Schema getSchema()
  {
    if (exprs[0].getSchema().is(NULL).maybe())
    {
      return SchemaFactory.arrayOrNullSchema();
    }
    else
    {
      return SchemaFactory.arraySchema();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  protected JsonIterator iterRaw(final Context context) throws Exception
  {
    final JsonRecord rec = (JsonRecord) exprs[0].eval(context);
    if (rec == null)
    {
      return JsonIterator.NULL; // TODO: should this be []?
    }
    
    return JsonRecord.keyValueIter(rec.iteratorSorted());
  }

}

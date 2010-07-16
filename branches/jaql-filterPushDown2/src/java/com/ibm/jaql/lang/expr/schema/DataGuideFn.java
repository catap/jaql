/*
 * Copyright (C) IBM Corp. 2010.
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
package com.ibm.jaql.lang.expr.schema;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.StringSchema;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * Usage: dataGuide(any value) returns [string]
 * 
 * Return a string that represents each unique path in the value.
 * For records:
 *    yield ""
 *    for each field:value in record:
 *       yield "." + field + dataGuide(value)
 * For arrays:
 *    yield "[]"
 *    for each value in array
 *       yield "[]" + dataGuide(value) 
 * For atomic types:
 *     yield ":" + the type name, eg, ":string", ":null"
 */
public class DataGuideFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public static final Schema schema = new ArraySchema(null, new StringSchema());
      
    public Descriptor()
    {
      super("dataGuide", DataGuideFn.class);
    }
  }
  
  /**
   * @param args
   */
  public DataGuideFn(Expr[] args)
  {
    super(args);
  }

  @Override
  public Schema getSchema()
  {
    return Descriptor.schema;
  }
  
  // TODO: make a JsonValueWalker and build result lazily
  public static void dataGuide(SpilledJsonArray result, String prefix, JsonValue value)
    throws IOException
  {
    if( value instanceof JsonRecord )
    {
      result.add(new JsonString(prefix));
      JsonRecord rec = (JsonRecord)value;
      Iterator<Entry<JsonString, JsonValue>> iter = rec.iterator();
      while( iter.hasNext() )
      {
        Entry<JsonString, JsonValue> e = iter.next();
        JsonString field = e.getKey();  // TODO: add quotes if needed
        JsonValue fvalue = e.getValue();
        dataGuide(result, prefix + '.' + field, fvalue);
      }
    }
    else if( value instanceof JsonArray )
    {
      prefix = prefix + "[]";
      result.add(new JsonString(prefix));
      for( JsonValue v: (JsonArray)value )
      {
        dataGuide(result, prefix, v);
      }
    }
    else if( value == null )
    {
      result.add(new JsonString(prefix + ":null"));
    }
    else
    {
      JsonString type = value.getType().getName();
      result.add(new JsonString(prefix + ":" + type));
    }
  }

  // State
  protected SpilledJsonArray result;

  public JsonArray eval(final Context context) throws Exception
  {
    if( result == null )
    {
      result = new SpilledJsonArray();
    }
    else
    {
      result.clear();
    }
    dataGuide(result, "$", exprs[0].eval(context));
    return result;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(final Context context) throws Exception
  {
    JsonArray arr = eval(context);
    return arr.iter();
  }
}
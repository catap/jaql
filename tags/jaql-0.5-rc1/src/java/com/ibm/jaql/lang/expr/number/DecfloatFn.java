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
package com.ibm.jaql.lang.expr.number;

import static com.ibm.jaql.json.type.JsonType.NULL;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * @jaqlDescription Define a decfloat value
 * 
 * @jaqlExample decfloat(5m);
 * 5m
 * 
 * @jaqlExample 5m instanceof schema decfloat(value=5m);
 * true
 * 
 *  
 */
public class DecfloatFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("decfloat", DecfloatFn.class);
    }
  }
  
  /**
   * double(numeric or string)
   * 
   * @param exprs
   */
  public DecfloatFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param num
   */
  public DecfloatFn(Expr num)
  {
    super(num);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonDecimal eval(final Context context) throws Exception
  {
    JsonValue val = exprs[0].eval(context);
    if (val == null)
    {
      return null;
    }
    else if (val instanceof JsonDecimal)
    {
      return (JsonDecimal)val;
    }
    else if (val instanceof JsonNumber)
    {
      return new JsonDecimal(((JsonNumber)val).decimalValue()); // TODO: memory
    }
    else if (val instanceof JsonString)
    {
      return new JsonDecimal(val.toString()); // TODO: memory
    }
    else
    {
      throw new ClassCastException("cannot convert "
          + val.getEncoding().getType().name() + " to decfloat");
    }
  }
  
  @Override
  public Schema getSchema()
  {
    Schema in = exprs[0].getSchema();
    return in.is(NULL).never() ? SchemaFactory.decfloatSchema() : SchemaFactory.decfloatOrNullSchema();
  }
}

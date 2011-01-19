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
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * @jaqlDescription Get the double value of a numric value.
 * 
 * Usage:
 * double double(number A);
 * 
 * @jaqlExample 22d instanceof schema double;
 * true
 * 
 * @jaqlExample double(5);
 * 5.0
 * 
 * @jaqlExample double(5m);
 * 5.0
 * 
 * @jaqlExample double(5d);
 * 5.0
 */
public class DoubleFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("double", DoubleFn.class);
    }
  }
  
  /**
   * double(numeric or string)
   * 
   * @param exprs
   */
  public DoubleFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param num
   */
  public DoubleFn(Expr num)
  {
    super(num);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonDouble eval(final Context context) throws Exception
  {
    JsonValue val = exprs[0].eval(context);
    if (val == null)
    {
      return null;
    }
    else if (val instanceof JsonDouble)
    {
      return (JsonDouble)val;
    }
    else if (val instanceof JsonNumber)
    {
      JsonNumber n = (JsonNumber) val;
      val = new JsonDouble(n.doubleValue()); // TODO: memory
    }
    else if (val instanceof JsonString)
    {
      val = new JsonDouble(val.toString()); // TODO: memory
    }
    else
    {
      throw new ClassCastException("cannot convert "
          + val.getEncoding().getType().name() + " to double");
    }
    return (JsonDouble)val; // TODO: memory
  }
  
  @Override
  public Schema getSchema()
  {
    Schema in = exprs[0].getSchema();
    return in.is(NULL).never() ? SchemaFactory.doubleSchema() : SchemaFactory.doubleOrNullSchema();
  }
}

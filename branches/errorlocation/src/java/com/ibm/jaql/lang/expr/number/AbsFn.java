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

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * @jaqlDescription Return the absolute value of a numeric value
 * 
 * Usage:
 * number abs(number)
 * 
 * @jaqlExample abs(-100);
 * 100
 * 
 * @jaqlExample abs(-3.14)
 * 3.14
 * 
 */
public class AbsFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("abs", AbsFn.class);
    }
  }
  
  /**
   * @param exprs
   */
  public AbsFn(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  protected JsonNumber evalRaw(final Context context) throws Exception
  {
    JsonNumber v = (JsonNumber)exprs[0].eval(context);
  	if (v == null) {
  		return null;
  	} else if (v instanceof JsonDouble) {
    	return new JsonDouble(Math.abs(v.doubleValue()));
    } else if (v instanceof JsonLong) {
    	// DISABLED DUE TO TYPE PROMOTION RULES: -Long.MIN_VALUE does not fit into a long --> convert to JDecimal	
    	return new JsonLong(Math.abs(v.longValue()));
    } else { 
    	// input type is JDecimal or JLong (w/ minimum value)
    	return new JsonDecimal(v.decimalValue().abs()); // TODO: reuse
    }
  }  
  
  @Override
  public Schema getSchema()
  {
    Schema in = exprs[0].getSchema();
    Schema out = SchemaTransformation.restrictToNumberTypesOrNull(in);
    if (out == null)
    {
      throw new RuntimeException("abs expects number as input");
    }
    return out;
  }
}

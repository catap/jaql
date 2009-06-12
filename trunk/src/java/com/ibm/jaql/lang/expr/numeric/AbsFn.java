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
package com.ibm.jaql.lang.expr.numeric;

import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumeric;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "abs", minArgs = 1, maxArgs = 1)
public class AbsFn extends Expr
{
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
  public JsonNumeric eval(final Context context) throws Exception
  {
  	JsonNumeric v = (JsonNumeric)exprs[0].eval(context);
  	if (v == null) {
  		return null;
  	} else if (v instanceof JsonDouble) {
    	return new JsonDouble(Math.abs(v.doubleValue()));
    } else if (v instanceof JsonLong && v.longValue() != Long.MIN_VALUE) {
    	// -Long.MIN_VALUE does not fit into a long --> convert to JDecimal	
    	return new JsonLong(Math.abs(v.longValue()));
    } else { 
    	// input type is JDecimal or JLong (w/ minimum value)
    	return new JsonDecimal(v.decimalValue().abs()); // TODO: reuse
    }
  }  
}

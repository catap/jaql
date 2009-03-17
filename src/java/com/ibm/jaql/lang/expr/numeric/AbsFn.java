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

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JDecimal;
import com.ibm.jaql.json.type.JDouble;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JNumeric;
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
  public Item eval(final Context context) throws Exception
  {
  	JNumeric v = (JNumeric)exprs[0].eval(context).get();
  	if (v == null) {
  		return Item.nil;
  	} else if (v instanceof JDouble) {
    	return new Item(new JDouble(Math.abs(v.doubleValue()))); // TODO: reuse
    } else if (v instanceof JLong && v.longValue() != Long.MIN_VALUE) {
    	// -Long.MIN_VALUE does not fit into a long --> convert to JDecimal	
    	return new Item(new JLong(Math.abs(v.longValue()))); // TODO: reuse
    } else { 
    	// input type is JDecimal or JLong (w/ minimum value)
    	return new Item(new JDecimal(v.decimalValue().abs())); // TODO: reuse
    }
  }  
}

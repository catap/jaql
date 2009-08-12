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
package com.ibm.jaql.lang.expr.random;

import java.util.Map;
import java.util.Random;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.MutableJsonDouble;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "randomDouble", minArgs = 0, maxArgs = 1)
public class RandomDoubleFn extends Expr
{
  private Random rng;
  private MutableJsonDouble jdouble = new MutableJsonDouble();

  /**
   * long randomLong(number seed)
   * 
   * @param exprs
   */
  public RandomDoubleFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param seed
   */
  public RandomDoubleFn()
  {
    super(NO_EXPRS);
  }

  /**
   * @param seed
   */
  public RandomDoubleFn(Expr seed)
  {
    super(seed);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isConst()
   */
  @Override
  public Map<ExprProperty, Boolean> getProperties()
  {
    Map<ExprProperty, Boolean> result = ExprProperty.createUnsafeDefaults();
    result.put(ExprProperty.IS_NONDETERMINISTIC, true);
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonDouble eval(final Context context) throws Exception
  {
    if (rng == null)
    {
      if( exprs.length == 0 )
      {
        rng = new Random();
      }
      else
      {
        JsonNumber seedItem = (JsonNumber) exprs[0].eval(context);
        long seed = seedItem.longValue();
        rng = new Random(seed);
      }
    }
    jdouble.set(rng.nextDouble());
    return jdouble;
  }
  
  @Override
  public Schema getSchema()
  {
    return SchemaFactory.doubleSchema();
  }
}

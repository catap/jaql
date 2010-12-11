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
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * @jaqlDescription return a uniformly distributed long value 
 * Usage:
 * long randomLong( long? seed )
 * 
 * The optional seed parameter is used to seed the internally used random number generator.
 * 
 * Note: randomLong will produce a pseudo-random sequence of longs when called in sequence.
 * If its called by multiple processes, in parallel (as done in MapReduce), then there are no
 * guarantees (and in fact, if all sequential instances use the same seed, you'll get common
 * prefixes). 
 */
public class RandomLongFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par01
  {
    public Descriptor()
    {
      super("randomLong", RandomLongFn.class);
    }
  }
  
  private Random rng;
  private MutableJsonLong  longType = new MutableJsonLong();

  /**
   * long randomLong(number seed)
   * 
   * @param exprs
   */
  public RandomLongFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param seed
   */
  public RandomLongFn(Expr seed)
  {
    super(new Expr[]{seed});
  }

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
  public JsonLong eval(final Context context) throws Exception
  {
    // FIXME: This class does not work in recursion...
    if (rng == null)
    {
      JsonNumber seedItem = (JsonNumber) exprs[0].eval(context);
      if (seedItem != null)
      {
        long seed = seedItem.longValue();
        rng = new Random(seed);
      }
      else
      {
        rng = new Random();
      }
    }
    longType.set(rng.nextLong() & 0x7FFFFFFFFFFFFFFFL);
    return longType;
  }
  
  @Override
  public Schema getSchema()
  {
    return SchemaFactory.longSchema();
  }
}

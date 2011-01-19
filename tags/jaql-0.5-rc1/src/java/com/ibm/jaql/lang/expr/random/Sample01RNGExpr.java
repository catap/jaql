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
import com.ibm.jaql.json.type.JsonEncoding;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonDouble;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.registry.RNGStore;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * @jaqlDescription return a uniformly distributed double value between 0.0 (inclusive) and 1.0 (exclusive) from a registered RNG
 * Usage:
 * double sample01RNG( string key )
 * 
 * An RNG associated with key must have been previously registered using registerRNG.
 */
public class Sample01RNGExpr extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("sample01RNG", Sample01RNGExpr.class);
    }
  }
  
  private MutableJsonDouble value = new MutableJsonDouble();
  /**
   * @param exprs
   */
  public Sample01RNGExpr(Expr[] exprs)
  {
    super(exprs);
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
  @Override
  public JsonDouble eval(Context context) throws Exception
  {
    JsonValue key = exprs[0].eval(context);

    //RNGStore.RNGEntry entry = RNGStore.get(key);
    RNGStore.RNGEntry entry = JaqlUtil.getRNGStore().get(key);
    if (entry == null)
    {
      return null;
    }

    // use a different rng here if needed...
    Random rng = (Random) entry.getRng();
    if (rng == null)
    {
      Function f = entry.getSeed();
      f.setArguments(new JsonValue[0]); // empty
      JsonValue seedValue = f.eval(context);
      long seed = 0;
      if (seedValue.getEncoding() == JsonEncoding.LONG)
      {
        seed = ((JsonNumber) seedValue).longValue();
      }
      else if (seedValue.getEncoding() == JsonEncoding.STRING)
      {
        seed = Long.parseLong(((JsonString) seedValue).toString());
      }
      else
      {
        throw new RuntimeException("seed is of invalid type");
      }
      rng = new Random(seed);
      entry.setRng(rng);
    }
    value.set(rng.nextDouble());
    return value;
  }


  @Override
  public Schema getSchema() {
    return SchemaFactory.doubleOrNullSchema();
  }
  
  
}

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

import java.util.Random;

import com.ibm.jaql.json.type.JsonEncoding;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JaqlFunction;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.lang.registry.RNGStore;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * 
 */
@JaqlFn(fnName = "sampleRNG", minArgs = 1, maxArgs = 1)
public class SampleRNGExpr extends Expr
{
  /**
   * @param exprs
   */
  public SampleRNGExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonLong eval(Context context) throws Exception
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
      JaqlFunction f = entry.getSeed();
      JsonValue seedValue = f.eval(context, new JsonValue[]{});
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
    return new JsonLong(rng.nextLong());
  }
}

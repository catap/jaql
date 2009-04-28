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

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JNumber;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JFunction;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.lang.registry.RNGStore;
import com.ibm.jaql.lang.registry.RNGStore.RNGEntry;
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
  public Item eval(Context context) throws Exception
  {
    Item key = exprs[0].eval(context);

    //RNGStore.RNGEntry entry = RNGStore.get(key);
    RNGStore.RNGEntry entry = JaqlUtil.getRNGStore().get(key);
    if (entry == null)
    {
      return Item.NIL;
    }

    Item val = new Item();
    // use a different rng here if needed...
    Random rng = (Random) entry.getRng();
    if (rng == null)
    {
      JFunction f = entry.getSeed();
      Item seedItem = f.eval(context, new Item[]{});
      long seed = 0;
      if (seedItem.getEncoding() == Item.Encoding.LONG)
      {
        seed = ((JNumber) seedItem.get()).longValue();
      }
      else if (seedItem.getEncoding() == Item.Encoding.STRING)
      {
        seed = Long.parseLong(((JString) seedItem.get()).toString());
      }
      else
      {
        throw new RuntimeException();
      }
      rng = new Random(seed);
      entry.setRng(rng);
    }
    val.set(new JLong(rng.nextLong()));
    return val;
  }
}

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
package com.ibm.jaql.lang.registry;

import java.io.InputStream;

import antlr.collections.impl.BitSet;

import com.ibm.jaql.io.registry.JsonRegistryFormat;
import com.ibm.jaql.io.registry.Registry;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.util.JIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;

/** Like JsonRegistryFormat but is able to read JAQL scripts.
 * 
 * @param <K>
 * @param <V>
 */
public abstract class JaqlRegistryFormat<K, V> extends JsonRegistryFormat<K, V>
{

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.registry.JsonRegistryFormat#readRegistry(java.io.InputStream,
   *      com.ibm.jaql.io.registry.Registry)
   */
  @Override
  public void readRegistry(InputStream input, Registry<K, V> registry)
      throws Exception
  {
    Context ctx = new Context();
    JaqlLexer lexer = new JaqlLexer(input);
    JaqlParser parser = new JaqlParser(lexer);
    boolean parsing = true;

    try
    {

      while (true)
      {
        parsing = true;
        Expr expr = parser.parse();
        // FIXME: restrict exprs to be of type RegisterAdapterExpr
        parsing = false;
        if (parser.done) break;
        if (expr == null) continue;

        // expect a JArray of JRecord
        JArray arr = (JArray) expr.eval(ctx).get();
        for (JIterator iter = arr.jiterator(); iter.moveNext();)
        {
          JRecord r = (JRecord) iter.current();
          K kVal = convertKey(r);
          V vVal = convertVal(r);
          registry.register(kVal, vVal);
        }
      }
    }
    catch (Exception ex)
    {
      System.err.println(ex);
      ex.printStackTrace();
      System.err.flush();
      if (parsing)
      {
        BitSet bs = new BitSet();
        bs.add(JaqlParser.EOF);
        bs.add(JaqlParser.SEMI);
        parser.consumeUntil(bs);
      }
    }
    finally
    {
      ctx.endQuery(); // TODO: need to wrap up parse, eval, cleanup into one class and use everywhere
    }
  }

}

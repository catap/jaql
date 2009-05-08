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
package com.ibm.jaql.lang.expr.span;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JSpan;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.lang.util.SimpleTokenizer;

/**
 * 
 */
@JaqlFn(fnName = "tokenize", minArgs = 1, maxArgs = 1)
public class TokenizeFn extends IterExpr // TODO: make much faster and better!
{
  protected Expr expr1;

  /**
   * @param exprs
   */
  public TokenizeFn(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public Iter iter(final Context context) throws Exception
  {
    final JString text = (JString) exprs[0].eval(context).get();
    if (text == null)
    {
      return Iter.nil;
    }
    return new Iter() {
      JString               tokText   = new JString();
      Item                  item      = new Item(tokText);

      final SimpleTokenizer tokenizer = new SimpleTokenizer(text.getInternalBytes(), 0,
                                          text.getLength()); // TODO: reuse
      public Item next() throws Exception
      {
        JSpan span = tokenizer.next();
        if (span == null)
        {
          return null;
        }
        span.getText(text, tokText);
        return item;
      }
    };
  }
}

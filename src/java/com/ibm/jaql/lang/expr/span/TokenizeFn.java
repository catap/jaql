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

import com.ibm.jaql.json.type.JsonSpan;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.SubJsonString;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.util.SimpleTokenizer;

/**
 * 
 */
public class TokenizeFn extends IterExpr // TODO: make much faster and better!
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("tokenize", TokenizeFn.class);
    }
  }
  
  final SubJsonString tokText = new SubJsonString();
  final SimpleTokenizer tokenizer = new SimpleTokenizer();


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
  protected JsonIterator iterRaw(final Context context) throws Exception
  {
    final JsonString text = (JsonString) exprs[0].eval(context);
    if (text == null)
    {
      return JsonIterator.NULL;
    }
    tokenizer.reset(text.getInternalBytes(), text.bytesOffset(), text.bytesLength());
    
    return new JsonIterator(tokText) {

      protected boolean moveNextRaw() throws Exception
      {
        JsonSpan span = tokenizer.next();
        if (span == null)
        {
          return false;
        }
        text.substring(tokText, (int)span.begin, (int)(span.end-span.begin));
        return true; // currentValue == tokText
      }
    };
  }
}

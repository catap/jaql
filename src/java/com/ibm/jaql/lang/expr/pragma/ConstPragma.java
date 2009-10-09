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
package com.ibm.jaql.lang.expr.pragma;

import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.MacroExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.rewrite.VarTagger;

/**
 * This is a pragma function to force const evaluation.
 */
public class ConstPragma extends MacroExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("const", ConstPragma.class);
    }
  }
  
  /**
   * @param exprs
   */
  public ConstPragma(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public Expr expand(Env env) throws Exception
  {
    // we should check for compile time computability here; this is currently commented out
    // because it blocks function defs of form (a=5, fn() a)    
//    if (!exprs[0].isCompileTimeComputable().always())
//    {
//      throw new IllegalArgumentException("argument to const() cannot be evaluated at compile time");
//    }
    try
    {
      VarTagger.tag(exprs[0]);
      return new ConstExpr(exprs[0].eval(Env.getCompileTimeContext()));
    }
    catch (Exception e)
    {
      throw new IllegalArgumentException(e);
    }
  }

}

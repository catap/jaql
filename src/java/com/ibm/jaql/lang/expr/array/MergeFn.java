/*
 * Copyright (C) IBM Corp. 2009.
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
package com.ibm.jaql.lang.expr.array;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.MacroExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * DEPRECATED: use union instead.
 * Union multiple arrays into one array in arbitrary order without
 * removing duplicates (like SQL's UNION ALL) 
 */
public class MergeFn extends MacroExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par1u
  {
    public Descriptor()
    {
      super("merge", MergeFn.class);
    }
  }
  
  public MergeFn(Expr... inputs)
  {
    super(inputs);
  }
  
  @Override
  public Expr expandRaw(Env env) throws Exception
  {
    return new UnionFn(exprs);
  }
}

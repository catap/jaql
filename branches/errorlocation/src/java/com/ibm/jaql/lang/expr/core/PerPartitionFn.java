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
package com.ibm.jaql.lang.expr.core;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.Function;

public class PerPartitionFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("perPartition", PerPartitionFn.class);
    }
  }
  
  public PerPartitionFn(Expr[] inputs)
  {
    super(inputs);
  }
  
  /**
   * 
   * @param inputs [BindingExpr, Expr+]
   */
  public PerPartitionFn(Expr input, Expr fn)
  {
    super(input, fn);
  }
  
  /**
   * This expression can be applied in parallel per partition of child i.
   */
  @Override
  public boolean isMappable(int i)
  {
    return i == 0;
  }

  @Override
  protected JsonIterator iterRaw(final Context context) throws Exception
  {
    Function fn = (Function)exprs[1].eval(context);
    fn.setArguments(exprs[0]);
    return fn.iter(context);
  }
}

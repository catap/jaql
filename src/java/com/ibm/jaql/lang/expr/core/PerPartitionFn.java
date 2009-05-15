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
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JFunction;

@JaqlFn(fnName="perPartition", minArgs=2, maxArgs=2)
public class PerPartitionFn extends IterExpr
{

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
  public Iter iter(final Context context) throws Exception
  {
    JFunction fn = (JFunction)exprs[1].eval(context).get();
    return fn.iter(context, new Expr[] {exprs[0]});
  }
}

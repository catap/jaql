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
package com.ibm.jaql.job;

import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;


public class JaqlStage extends Stage
{
  protected Context context;
  protected Expr expr;

  public JaqlStage(JobGraph graph, Context context, Expr expr)
  {
    super(graph);
    this.context = context;
    this.expr = expr;
  }

  @Override
  public void runStage() throws Exception
  {
    // The result is discarded; it is most likely just a variable name or file handle
    if( expr.getSchema().isArrayOrNull().always() )
    {
      JsonIterator iter = expr.iter(context);
      while (iter.moveNext())
      {
        // ignore item
      }
    }
    else
    {
      expr.eval(context);
    }
//    Context context = new Context();
//    if( expr.isArray().always() )
//    {
//      Iter iter = expr.iter(context);
//      iter.print(out);
//    }
//    else
//    {
//      Item item = expr.eval(context);
//      item.print(out);
//    }
  }

}

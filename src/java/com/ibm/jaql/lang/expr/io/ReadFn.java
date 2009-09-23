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
package com.ibm.jaql.lang.expr.io;

import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * An expression used for reading data into jaql. It is called as follows:
 * read({type: '...', location: '...', inoptions: {...}}) <br>
 * The type specifies which InputAdapter to use, the location specifies the
 * address from which the adapter will read. The optional inoptions further
 * parameterize the adapter's behavior. <br>
 * If inoptions are not specified, then default options that are registered for
 * the type at the AdapterStore will be used. If no options are specified and
 * there are no defaults registered, it is an error. If both options are
 * specified and default options are registered, then the union of option fields
 * will be used. If there are duplicate names, then the query options will be
 * used as an override.
 */
public final class ReadFn extends AbstractReadExpr implements PotentialMapReducible
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("read", ReadFn.class);
    }
  }
  
  /**
   * @param exprs
   */
  public ReadFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param e
   */
  public ReadFn(Expr e)
  {
    super(new Expr[]{e});
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.PotentialMapReducible#isMapReducible()
   */
  public boolean isMapReducible()
  {
    return MapReducibleUtil.isMapReducible(true, exprs[0]);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.PotentialMapReducible#rewriteToMapReduce(com.ibm.jaql.lang.Expr)
   */
//  public Expr rewriteToMapReduce(Expr expr)
//  {
//    if (exprs[0] instanceof RecordExpr && expr instanceof RecordExpr)
//      return MapReducibleUtil.rewriteToMapReduce((RecordExpr) exprs[0],
//          (RecordExpr) expr);
//    return exprs[0];
//  }
}

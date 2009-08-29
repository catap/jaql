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
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * An expression used for writing external data. It is called as follows:
 * write({type: '...', location: '...', outoptions: '...', inoptions: '...'},
 * expr); <br>
 * The type specifies which OutputAdapter to use, the location specifies the
 * address to which the adapter will write. The optional outoptions further
 * parametrize the adapter's behavior. The optional inoptions can be used to
 * parametrize a read expression that takes as input a write expression (e.g.,
 * read(write({...}, expr)) ). <br>
 * If outoptions or inoptions are not specified, then default options that are
 * registered for the type at the AdapterStore will be used. If no options are
 * specified and there are no defaults registered, it is an error. If both
 * options are specified and default options are registered, then the union of
 * option fields will be used. If there are duplicate names, then the query
 * options will be used as an override.
 */
@JaqlFn(fnName = "write", minArgs = 2, maxArgs = 2)
public final class WriteFn extends AbstractWriteExpr implements PotentialMapReducible
{
  /**
   * @param exprs
   */
  public WriteFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param toWrite
   * @param fd
   */
  public WriteFn(Expr toWrite, Expr fd)
  {
    super(toWrite, fd);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.PotentialMapReducible#isMapReducible()
   */
  public boolean isMapReducible()
  {
    return MapReducibleUtil.isMapReducible(true, descriptor());
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.PotentialMapReducible#rewriteToMapReduce(com.ibm.jaql.lang.Expr)
   */
//  public Expr rewriteToMapReduce(Expr expr)
//  {
//    if (descriptor() instanceof RecordExpr && expr instanceof RecordExpr)
//      return MapReducibleUtil.rewriteToMapReduce((RecordExpr) descriptor(),
//          (RecordExpr) expr);
//    return descriptor();
//  }

}

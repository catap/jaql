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
 * An expression used for writing external data. It is called as follows:
 * 
 * <pre>
 * write({type: '...', 
 *        location: '...', 
 *        outoptions: '...', 
 *        inoptions: '...'}
 *       , expr);
 * </pre>
 * 
 * The <tt>type</tt> specifies which {@link OutputAdapter} to use, the
 * <tt>location</tt> specifies the address to which the adapter will write. The
 * optional <tt>outoptions</tt> further parameterize the adapter's behavior. The
 * optional <tt>inoptions</tt> can be used to parametrize a read expression that
 * takes as input a write expression (e.g., <tt>read(write({...}, expr)) </tt>).
 * <p>
 * If <tt>outoptions</tt> or <tt>inoptions</tt> are unspecified, then default
 * options that are registered for the type at the {@link AdapterStore} will be
 * used. If no options are specified and there are no defaults registered, it is
 * an error. If both options are specified and default options are registered,
 * then the union of option fields will be used. If there are duplicate names,
 * then the query options will be used as an override.
 * 
 * @see AdapterStore
 */
public final class WriteFn extends AbstractWriteExpr implements PotentialMapReducible
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("write", WriteFn.class);
    }
  }
  
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
    return MapReducibleUtil.isMapReducible(false, descriptor());
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

/*
 * Copyright (C) IBM Corp. 2010.
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
package com.ibm.jaql.lang;

import java.io.Closeable;
import java.io.IOException;

import com.ibm.jaql.lang.expr.core.Expr;

/** Explain an <code>Expr</code> tree using another tree
 * 
 * If the instance is also <code>Closable</code>, then close() should be called.
 */ 
public abstract class ExplainHandler implements Closeable
{
  /** 
   * Transform <code>expr</code> into a JSON string or JSON
   * data structure that represents <code>expr</code>, which is
   * returned to the client. Alternatively, perform any 
   * operation to explain <code>expr</code> and return <code>null</code> 
   * to avoid returning anything to the client.
   */
  public abstract Expr explain(Expr expr) throws Exception;
  
  /** Close any underlying resources of this handler. */
  @Override
  public void close() throws IOException
  {
  }
}

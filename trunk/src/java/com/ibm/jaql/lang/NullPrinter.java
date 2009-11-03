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
package com.ibm.jaql.lang;

import java.io.IOException;

import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;

public class NullPrinter implements JaqlPrinter
{
  private static final NullPrinter THE_INSTANCE = new NullPrinter();
  
  public static NullPrinter get()
  {
    return THE_INSTANCE;
  }
  
  private NullPrinter() { };
  
  @Override
  public void close() throws IOException
  {
  }

  @Override
  public void print(Expr expr, Context context) throws Exception
  {
  }

  @Override
  public void printPrompt() throws IOException
  {
  }
}

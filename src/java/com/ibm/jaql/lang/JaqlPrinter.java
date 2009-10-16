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

/**
 * For printing of output in JAQL shell.
 */
public interface JaqlPrinter {
  /**
   * Prints the evaluated value of the given JAQL expression.
   * 
   * @param expr An expression
   * @param context Context
   * @throws Exception
   */
  public void print(Expr expr, Context context) throws Exception;

  /**
   * Prints the JAQL shell prompt.
   * 
   * @throws IOException
   */
  public void printPrompt() throws IOException;

  /**
   * Closes this printer.
   * 
   * @throws IOException
   */
  public void close() throws IOException;
}
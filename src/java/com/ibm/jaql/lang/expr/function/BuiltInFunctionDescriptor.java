/*
 * Copyright (C) IBM Corp. 2008.
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
package com.ibm.jaql.lang.expr.function;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.lang.expr.core.Expr;

/** Descriptor for built-in functions. */
public interface BuiltInFunctionDescriptor
{
  /** Returns the name of the function. */
  public String getName();
  
  /** Returns a description of the formal parameters of the function */
  public JsonValueParameters getParameters();
  
  /** Derives the schema of a result of the function call with the specified arguments.
   * This schema is mainly used for documentation purposes; during compilation, 
   * {@link Expr#getSchema()} will be used. */
  public Schema getSchema();
  
  /** Returns the class that implements the function */
  public Class<? extends Expr> getImplementingClass();
  
  /** Constructs an expression for calling the function with the specified arguments. */
  public Expr construct(Expr[] positionalArgs);
}

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

import java.util.HashSet;

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.util.FastPrinter;


public class DefaultExpr extends PseudoExpr
{
  public DefaultExpr(Expr[] exprs)
  {
    super(exprs);
  }

  public DefaultExpr()
  {
    super(NO_EXPRS);
  }

//  @Override
//  public Item eval(Context context) throws Exception
//  {
//    return Item.nil;
//  }
  
  public void decompile(FastPrinter exprText, HashSet<Var> capturedVars) throws Exception
  {
    exprText.print("default");
  }
}

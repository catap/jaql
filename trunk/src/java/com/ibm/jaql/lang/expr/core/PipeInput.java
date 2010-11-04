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

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.util.FastPrinter;

/**
 * The purpose of this class is to hide the printing of the pipeline variable inside of subpipes
 * like Partition and Tee.
 *
 */
public class PipeInput extends IterExpr
{
  // TODO: PipeInput could contain the variable instead of a VarExpr below.
  // TODO: Alternatively, we could connect pipelines from source to sink instead of functionally.
  
  public PipeInput(Expr[] exprs)
  {
    super(exprs);
    assert exprs[0] instanceof VarExpr;
  }

  /**
   * 
   * @param expr0 VarExpr
   */
  public PipeInput(Expr expr0)
  {
    super(expr0);
    assert exprs[0] instanceof VarExpr;
  }

  @Override
  public Schema getSchema()
  {
    return exprs[0].getSchema();
  }
  
  @Override
  public void decompile(FastPrinter exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    assert exprs[0] instanceof VarExpr;
    // Print nothing.
  }

  @Override
  public JsonIterator iter(Context context) throws Exception
  {
    return exprs[0].iter(context);
  }
}

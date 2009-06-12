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

import java.io.PrintStream;
import java.util.HashSet;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JComparator;
import com.ibm.jaql.lang.core.Var;


public class CmpSingle extends CmpExpr // TODO: merge CmpSingle and CmpSpec?
{
  /**
   * 
   * @param exprs
   */
  public CmpSingle(Expr[] exprs)
  {
    super(exprs);
  }
  
  /**
   * Order by a single expression.
   * 
   * @param spec
   */
  public CmpSingle(CmpSpec spec)
  {
    super(spec);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprs[0].decompile(exprText, capturedVars);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(Context context) throws Exception
  {
    return exprs[0].eval(context);
  }
  
  public JComparator getComparator(Context context)
  {
    return ((CmpSpec)exprs[0]).getComparator(context);
  }
}

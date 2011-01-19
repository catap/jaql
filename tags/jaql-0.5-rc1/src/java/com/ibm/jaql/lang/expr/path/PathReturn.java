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
package com.ibm.jaql.lang.expr.path;

import java.util.HashSet;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.util.Bool3;
import com.ibm.jaql.util.FastPrinter;

/** End marker for list of path steps */
public class PathReturn extends PathStep
{
  /**
   * @param exprs
   */
  public PathReturn(Expr[] exprs)
  {
    super(exprs);
  }

  public PathReturn()
  {
    super(NO_EXPRS);
  }
  
  /**
   * 
   * @return
   */
  public PathStep nextStep()
  {
    return null;
  }

  /**
   * Make path.name into path{.name}
   */
  public void forceRecord()
  {
    Expr e = parent();
    if( e instanceof PathFieldValue )
    {
      Expr name = e.child(0);
      e.replaceInParent(new PathRecord(new PathOneField(name),this));
    }
  }

  @Override
  public boolean rewriteFirstStep() throws Exception
  {
    PathExpr pe = (PathExpr)parent;
    pe.replaceInParent(pe.child(0));
    return true;
  }

  /**
   * 
   */
  public void decompile(FastPrinter exprText, HashSet<Var> capturedVars)
  throws Exception
  {
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.PathExpr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonValue eval(Context context) throws Exception
  {
    return input;
  }
  
  // -- schema ------------------------------------------------------------------------------------
  
  @Override
  public PathStepSchema getSchema(Schema inputSchema)
  {
    return new PathStepSchema(inputSchema, Bool3.TRUE);
  }
}

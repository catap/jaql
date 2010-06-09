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

import java.io.PrintStream;
import java.util.HashSet;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;


/** e.g. .a as used is ${.a,.b} */;
public class PathOneField extends PathFields
{

  /**
   * @param exprs
   */
  public PathOneField(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param name
   */
  public PathOneField(Expr name)
  {
    super(name,new PathReturn());
  }

  /**
   * 
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
  throws Exception
  {
    exprText.print(".(");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print(")");
    exprs[1].decompile(exprText, capturedVars);
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.PathFields#matches(com.ibm.jaql.lang.core.Context, com.ibm.jaql.json.type.JString)
   */
  @Override
  public boolean matches(Context context, JsonString name) throws Exception
  {
    JsonString n = (JsonString)exprs[0].eval(context);
    if( n == null )
    {
      return false;
    }
    return n.equals(name);
  }
  
  // -- schema ------------------------------------------------------------------------------------
  
  @Override
  public PathStepSchema getSchema(Schema inputSchema)
  {
    return staticResolveField(inputSchema, exprs[0], nextStep());
  }
}

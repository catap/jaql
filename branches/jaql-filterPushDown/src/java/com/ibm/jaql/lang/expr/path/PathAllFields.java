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

import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.util.Bool3;


/**
 * @author kbeyer
 *
 */
public class PathAllFields extends PathFields
{

  /**
   * @param exprs
   */
  public PathAllFields(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   */
  public PathAllFields()
  {
    super(new PathReturn());
  }

  /**
   * 
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
  throws Exception
  {
    exprText.print("*");
    exprs[0].decompile(exprText, capturedVars);
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.PathFields#matches(com.ibm.jaql.lang.core.Context, com.ibm.jaql.json.type.JString)
   */
  @Override
  public boolean matches(Context context, JsonString name) throws Exception
  {
    return true;
  }
  
  //special case: resulting fields nested in PathStepSchema.schema
  public PathStepSchema getSchema(Schema inputSchema)
  {
    if (inputSchema instanceof RecordSchema)
    {
      return new PathStepSchema(inputSchema, Bool3.TRUE);
    }
    return new PathStepSchema(SchemaFactory.recordSchema(), Bool3.TRUE);
  }
}

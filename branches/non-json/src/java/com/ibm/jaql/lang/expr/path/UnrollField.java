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

import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;


public class UnrollField extends UnrollStep
{
  /**
   * @param exprs
   */
  public UnrollField(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param field
   */
  public UnrollField(Expr field)
  {
    super(field);
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
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.ExpandStep#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonValue eval(Context context, JsonValue recValue) throws Exception
  {
    JsonRecord rec = (JsonRecord)recValue;
    if( rec == null )
    {
      return null;
    }
    JsonString ename = (JsonString)exprs[0].eval(context);
    if( ename == null )
    {
      return null;
    }
    return rec.get(ename);
  }
}

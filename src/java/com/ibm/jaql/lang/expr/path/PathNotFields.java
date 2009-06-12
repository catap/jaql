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
import java.util.ArrayList;
import java.util.HashSet;

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;


/**
 * @author kbeyer
 *
 */
public class PathNotFields extends PathFields
{
  private static Expr[] makeArgs(ArrayList<PathStep> fields)
  {
    Expr[] es = fields.toArray(new Expr[fields.size()+1]);
    es[fields.size()] = new PathReturn();
    return es;
  }

  /**
   * @param exprs
   */
  public PathNotFields(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   */
  public PathNotFields(ArrayList<PathStep> fields)
  {
    super(makeArgs(fields));
  }

  /**
   * 
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
  throws Exception
  {
    exprText.print("* -");
    String sep = " ";
    int n = exprs.length - 1;
    for(int i = 0 ; i < n ; i++)
    {
      exprText.print(sep);
      exprs[i].decompile(exprText, capturedVars);
      sep = ", ";
    }
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.PathFields#matches(com.ibm.jaql.lang.core.Context, com.ibm.jaql.json.type.JString)
   */
  @Override
  public boolean matches(Context context, JsonString name) throws Exception
  {
    
    int n = exprs.length - 1;
    for(int i = 0 ; i < n ; i++)
    {
      PathOneField f = (PathOneField)exprs[i];
      if( f.matches(context, name) )
      {
        return false;
      }
    }
    return true;
  }
}
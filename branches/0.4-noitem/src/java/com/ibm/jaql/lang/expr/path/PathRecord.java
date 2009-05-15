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

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;


/**
 * @author kbeyer
 *
 */
public class PathRecord extends PathStep
{
  /**
   * @param exprs
   */
  public PathRecord(Expr[] exprs)
  {
    super(exprs);
  }

  private static ArrayList<PathStep> addRet(ArrayList<PathStep> fields)
  {
    fields.add(new PathReturn());
    return fields;
  }

  public PathRecord(PathFields fields, PathStep next)
  {
    super(fields, next);
  }

  public PathRecord(ArrayList<PathStep> fields) // Must be PathFields
  {
    super(addRet(fields));
  }
  
  /**
   * 
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
  throws Exception
  {
    exprText.print("{");
    String sep = " ";
    int m = exprs.length - 1;
    for(int i = 0 ; i < m ; i++)
    {
      exprText.print(sep);
      exprs[i].decompile(exprText, capturedVars);
      sep = ", ";
    }
    exprText.print(" }");
    exprs[m].decompile(exprText, capturedVars);
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.PathExpr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonValue eval(Context context) throws Exception
  {
    JsonRecord oldRec = (JsonRecord)input;
    if( oldRec == null )
    {
      return null;
    }
    // TODO: this can be made much faster when "*" is not used.
    // TODO: this can be made much faster when only "*" is used, without inclusion/exclusion
    BufferedJsonRecord newRec = new BufferedJsonRecord(); // TODO: memory
    final int n = oldRec.arity();
    final int m = exprs.length - 1;
    for( int i = 0 ; i < n ; i++ )
    {
      JsonString name = oldRec.getName(i);
      int j;
      for( j = 0 ; j < m ; j++ )
      {
        PathFields f = (PathFields)exprs[j];
        if( f.matches(context, name) )
        {
          JsonValue value = oldRec.getValue(i);
          value = f.nextStep(context, value);
          value = nextStep(context, value);
          newRec.add(name, value);
          j++;
          break;
        }
      }
      for( ; j < m ; j++ )
      {
        PathFields f = (PathFields)exprs[j];
        if( f.matches(context, name) && (j < m - 1 || f instanceof PathOneField) )
        {
          throw new RuntimeException("duplicate field name: " + name);
        }
      }
    }
    return newRec;
  }
}

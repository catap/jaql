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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

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
  
  // -- schema ------------------------------------------------------------------------------------
  
  // special case: resulting fields nested in PathStepSchema.schema
  @Override
  public PathStepSchema getSchema(Schema inputSchema)
  {
    if (inputSchema instanceof RecordSchema)
    {
      // gather all names
      Set<JsonString> removedFields = new HashSet<JsonString>();
      boolean unresolved = false;
      for(int i = 0 ; i < exprs.length-1; i++)
      {
        PathOneField f = (PathOneField)exprs[i];
        PathStepSchema s = f.getSchema(inputSchema);
        if (s.name != null)
        {
          removedFields.add(s.name);
        }
        else
        {
          unresolved = true;
        }
      }
      
      // and copy the input schema w/o those names
      List<RecordSchema.Field> fields = new LinkedList<RecordSchema.Field>();
      for (RecordSchema.Field field : ((RecordSchema)inputSchema).getFieldsByPosition())
      {
        if (!removedFields.contains(field.getName()))
        {
          fields.add(new RecordSchema.Field(field.getName(), field.getSchema(),
              unresolved ? true : field.isOptional())); // unresolved fields might match this field
        }
      }
      Schema rest = ((RecordSchema)inputSchema).getAdditionalSchema();
      return new PathStepSchema(new RecordSchema(fields, rest) , Bool3.TRUE);
    }
    return new PathStepSchema(SchemaFactory.recordSchema(), Bool3.TRUE);
  }
}

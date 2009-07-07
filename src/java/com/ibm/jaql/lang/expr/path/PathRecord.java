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

import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;


/** e.g. ${.a,.b} */;
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
  
  
  // -- schema ------------------------------------------------------------------------------------
  
  @Override
  public PathStepSchema getSchema(Schema inputSchema)
  {
    List<RecordSchema.Field> fields = new LinkedList<RecordSchema.Field>();
    Schema rest = null;
    for (int i=0; i<exprs.length-1; i++  )
    {
      PathFields f = (PathFields)exprs[i];
      PathStepSchema s = f.getSchema(inputSchema);
      if (s.hasData.maybe())
      {
        if (s.name != null)
        {
          RecordSchema.Field field = 
            new RecordSchema.Field(s.name, s.schema, s.hasData.always() ? false : true);
          fields.add(field);
        }
        else
        {
          if (f instanceof PathAllFields || f instanceof PathNotFields) 
          {
            // special case: nested record
            RecordSchema innerSchema = (RecordSchema)s.schema;
            for (RecordSchema.Field field : innerSchema.getFields())
            {
              fields.add(field);
            }
            rest = rest==null ? innerSchema.getRest() : SchemaTransformation.merge(rest, innerSchema.getRest());
          }
          else
          {
            // that's the usual case 
            rest = rest==null ? s.schema : SchemaTransformation.merge(rest, s.schema);
          }
        }
      }
    } // if it doesn't have data, ignore
    
    if (fields.size() > 0 || rest != null)
    {
      RecordSchema.Field[] fieldsArray = fields.toArray(new RecordSchema.Field[fields.size()]); 
      Schema mySchema = new RecordSchema(fieldsArray, rest); // always has data (maybe empty record)
      PathStepSchema nextSchema = nextStep().getSchema(mySchema);
      return nextSchema;
    }
    else
    {
      return nextStep().getSchema(SchemaFactory.emptyRecordSchema());
    }
  }
}

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

import java.util.ArrayList;

import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.StringSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.util.Bool3;

/** A step in a path expression */
public abstract class PathStep extends Expr
{
  /**
   * This value is set by the parent PathStep/PathExpr before eval()/iter() is called.
   */
  protected JsonValue input;
  
  /**
   * @param exprs
   */
  public PathStep(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr0
   */
  public PathStep(Expr expr0)
  {
    super(expr0);
  }

  /**
   * @param expr0
   * @param expr1
   */
  public PathStep(Expr expr0, Expr expr1)
  {
    super(expr0, expr1);
  }

  /**
   * @param expr0
   * @param expr1
   */
  public PathStep(Expr expr0, Expr expr1, Expr expr2)
  {
    super(expr0, expr1, expr2);
  }

  /**
   * 
   * @param exprs
   */
  public PathStep(ArrayList<PathStep> exprs)
  {
    super(exprs);
  }

  /**
   * Set the next step of the path expression
   * @param next
   */
  public void setNext(Expr next)
  {
    setChild(exprs.length-1, next);
  }
  
  /** Retrieve the next step of the path expression. 
   * 
   * @return
   */
  public PathStep nextStep()
  {
    return (PathStep)exprs[exprs.length-1];
  }

  /**
   * Make path.name into path{.name}
   */
  public void forceRecord()
  {
    nextStep().forceRecord();
  }

  /**
   * Get the last step.
   */
  public PathStep getReturn()
  {
    PathStep s = this;
    PathStep n = s.nextStep();
    while( n != null )
    {
      s = n;
      n = s.nextStep();
    }
    return s;
  }

  /**
   * Simplify the first step of a path expression.
   * (this.parent instanceof PathExpr)
   * 
   * Called by the rewrite engine when this step is the first step of a path expression.
   * It should only modify the PathExpr subtree - usually replacing the PathExpr.
   * 
   * Return true if the pathExpr subtree has been modified,
   *        false if the tree has not been modified.
   */
  public boolean rewriteFirstStep() throws Exception
  {
    return false;
  }
  
  /** 
   * 
   * @param context
   * @param input
   * @return
   * @throws Exception
   */
  protected JsonValue nextStep(Context context, JsonValue input) throws Exception
  {
    PathStep s = nextStep();
    s.input = input;
    return s.eval(context);
  }
  
  
  // -- schema ------------------------------------------------------------------------------------
  
  static class PathStepSchema
  {
    /** Schema of the value of the path step */
    final Schema schema;
    
    /** does the step return data? */
    final Bool3 hasData;
    
    /** if a field of a record, what's its name? null if unknown */
    final JsonString name;
    
    PathStepSchema(Schema schema, Bool3 hasData, JsonString name)
    {
      this.schema = schema;
      this.hasData = hasData;
      this.name = name;
    }
    
    PathStepSchema(Schema schema, Bool3 hasData)
    {
      this(schema, hasData, null);
    }
  }
  
  
  public abstract PathStepSchema getSchema(Schema inputSchema);
  
  static JsonString staticName(Expr nameExpr)
  {
    Schema nameSchema = nameExpr.getSchema();
    if (nameSchema instanceof StringSchema && nameSchema.isConstant())
    {
      return ((StringSchema)nameSchema).getConstant(); // non-null
    }
    return null;
    // TODO: other ways to find name? compile-time eval?
  }

  private static PathStepSchema staticResolveFieldShallow(JsonString name, RecordSchema recordSchema)
  {
    RecordSchema.Field field = recordSchema.getField(name);
    if (field != null)
    {
      return new PathStepSchema(field.getSchema(), field.isOptional() ? Bool3.UNKNOWN : Bool3.TRUE, name);
    }
    else
    {
      // field is null
      if (recordSchema.getAdditionalSchema() == null)
      {
        return new PathStepSchema(null, Bool3.FALSE, name); // means that this never produces output
      }
      else
      {
        return new PathStepSchema(recordSchema.getAdditionalSchema(), Bool3.UNKNOWN, name); // make optional
      }
    }
  }
  
  static PathStepSchema staticResolveField(Schema inputSchema, Expr nameExpr, PathStep nextStep)
  {
    if (inputSchema instanceof RecordSchema)
    {
      RecordSchema recordSchema = (RecordSchema)inputSchema;
      JsonString name = staticName(nameExpr);
      if (name != null)
      {
        PathStepSchema mySchema = staticResolveFieldShallow(name, recordSchema);
        if (mySchema.hasData.never())
        {
          return mySchema; // no need to look at next step
        }
        else
        {
          PathStepSchema nextSchema = nextStep.getSchema(mySchema.schema);
          return new PathStepSchema(nextSchema.schema, mySchema.hasData.and(nextSchema.hasData), name);
        }
      }
      else // field name could not be determined
      {
        PathStepSchema nextSchema = nextStep.getSchema(
            new RecordSchema(new RecordSchema.Field[0], recordSchema.elements()));
        return new PathStepSchema(nextSchema.schema, nextSchema.hasData.and(Bool3.UNKNOWN));
      }
    }
    PathStepSchema nextSchema = nextStep.getSchema(SchemaFactory.anySchema());
    return new PathStepSchema(nextSchema.schema, nextSchema.hasData.and(Bool3.UNKNOWN));
  }

}

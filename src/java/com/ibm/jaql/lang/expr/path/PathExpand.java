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

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.json.util.SingleJsonValueIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.util.Bool3;
import static com.ibm.jaql.json.type.JsonType.*;
/**
 * 
 *  Given ctx[]path
 *  if ctx always produces an array, and path produces an array for all elements of ctx,
 *  this can be rewritten
 * 
 * @author kbeyer
 */
public class PathExpand extends PathArray
{
  /**
   * @param exprs
   */
  public PathExpand(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param next
   */
  public PathExpand()
  {
    super(new PathReturn());
  }

  /**
   * @param next
   */
  public PathExpand(Expr next)
  {
    super(next);
  }

  /**
   * 
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
  throws Exception
  {
    exprText.print("[]");
    exprs[0].decompile(exprText, capturedVars);
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.PathExpr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    final JsonIterator outer;
    JsonValue val = input;
    if( val == null )
    {
      return JsonIterator.EMPTY;
    }
    else if( val instanceof JsonArray )
    {
      JsonArray arr = (JsonArray)input;
      outer = arr.iter();
    }
    else
    {
      outer = new SingleJsonValueIterator(input);
    }
    return new JsonIterator()
    {
      JsonIterator inner = JsonIterator.EMPTY;
      
      @Override
      public boolean moveNext() throws Exception
      {
        while( true )
        {
          if (inner.moveNext())
          {
            currentValue = inner.current();
            return true;
          }
          if (!outer.moveNext()) {
            return false;
          }
          JsonValue val = nextStep(context, outer.current());;
          if( val == null )
          {
            inner = JsonIterator.EMPTY;
          }
          else if( val instanceof JsonArray )
          {
            inner = ((JsonArray)val).iter();
          }
          else
          {
            inner = new SingleJsonValueIterator(val);
          }
        }
      }
    };
  }

  // -- schema ------------------------------------------------------------------------------------
  
  public PathStepSchema getSchema(Schema inputSchema)
  {
    // expand input arrays and remove nulls
    boolean mayReturnEmpty = inputSchema.isEmpty(ARRAY,NULL).maybe();
    Schema nextInput = SchemaTransformation.expandArrays(inputSchema);
    if (nextInput != null)
    {
      nextInput = SchemaTransformation.removeNullability(nextInput);
    }
    
    // expand arrays that result after next path step
    Schema nextResult = null;
    if (nextInput != null)
    {
      PathStepSchema next = nextStep().getSchema(nextInput);
      if (next.hasData.maybe())
      {
        mayReturnEmpty = mayReturnEmpty || next.hasData.maybeNot() 
            || next.schema.isEmpty(ARRAY,NULL).maybe();
        nextResult = SchemaTransformation.removeNullability(next.schema);
        if (nextResult != null)
        {
          nextResult = SchemaTransformation.expandArrays(nextResult); // null may come back in, but that's ok
        }
      }
    }
    
    // return result
    if (nextResult == null)
    {
      return new PathStepSchema(SchemaFactory.emptyArraySchema(), Bool3.TRUE);
    }
    else 
    {
      Schema arraySchema = new ArraySchema(null, nextResult);
      if (mayReturnEmpty)
      {
        arraySchema = SchemaTransformation.merge(arraySchema, SchemaFactory.emptyArraySchema());
      }
      return new PathStepSchema(arraySchema, Bool3.TRUE);
    }
  }
}

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

import static com.ibm.jaql.json.type.JsonType.ARRAY;
import static com.ibm.jaql.json.type.JsonType.NULL;

import java.util.HashSet;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.util.Bool3;
import com.ibm.jaql.util.FastPrinter;

public class PathArrayAll extends PathArray
{
  /**
   * @param exprs
   */
  public PathArrayAll(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   */
  public PathArrayAll()
  {
    super(new PathReturn());
  }

  /**
   * @param next
   */
  public PathArrayAll(Expr next)
  {
    super(next);
  }

  /**
   * 
   */
  protected void decompileRaw(FastPrinter exprText, HashSet<Var> capturedVars, boolean emitLocation)
  throws Exception
  {
    exprText.print("[*]");
    exprs[0].decompile(exprText, capturedVars,emitLocation);
  }


  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.PathExpr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  protected JsonIterator iterRaw(final Context context) throws Exception
  {
    final JsonArray arr = (JsonArray)input;
    if( arr == null )
    {
      return JsonIterator.EMPTY;
    }
    return new JsonIterator()
    {
      final JsonIterator iter = arr.iter();
      
      @Override
	protected boolean moveNextRaw() throws Exception
      {
        if (iter.moveNext()) 
        {
          currentValue = nextStep(context, iter.current());
          return true;
        }
        return false;
      }
    };
  }
  
  // -- schema ------------------------------------------------------------------------------------
  
  public PathStepSchema getSchema(Schema inputSchema)
  {
    PathStepSchema elements = null;
    boolean inputMaybeNull = false;
    if (inputSchema.is(ARRAY).never())
    {
      // TODO: this indicates a compile-time error but unless error handling is implemented, 
      // we are silent here
      return new PathStepSchema(null, Bool3.FALSE);
    }
    else if (inputSchema.is(ARRAY).always())
    {
      elements = nextStep().getSchema(inputSchema.elements());
    }
    else
    {
      Schema s = SchemaTransformation.restrictToArrayOrNull(inputSchema);
      if (s==null)  return new PathStepSchema(null, Bool3.FALSE); // TODO: friendly here as well
      inputMaybeNull = s.is(NULL).maybe();
      s = SchemaTransformation.removeNullability(s);
      elements = nextStep().getSchema(inputSchema.elements());
    }

    Schema result; 
    if (elements.hasData.always() && !inputMaybeNull)
    {
      result = new ArraySchema(null, elements.schema);
    }
    else if (elements.hasData.never())
    {
      result = SchemaFactory.emptyArraySchema();
    }
    else
    {
      result = SchemaTransformation.merge(new ArraySchema(null, elements.schema), 
          SchemaFactory.emptyArraySchema());
    }
      
    return new PathStepSchema(result, Bool3.TRUE);
  }
}

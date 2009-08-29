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
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.json.util.SingleJsonValueIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.util.Bool3;
import static com.ibm.jaql.json.type.JsonType.*;

/** [?] retains nulls */
public class PathToArray extends PathArray
{
  /**
   * @param exprs
   */
  public PathToArray(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   */
  public PathToArray()
  {
    super(new PathReturn());
  }

  /**
   * @param next
   */
  public PathToArray(Expr next)
  {
    super(next);
  }

  /**
   * 
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
  throws Exception
  {
    exprText.print("[?]");
    exprs[0].decompile(exprText, capturedVars);
  }
  
  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    JsonValue val = input;
    if( val == null )
    {
      return JsonIterator.NULL; // TODO: empty or nil?
    }
    final JsonIterator iter;
    if( val instanceof JsonArray )
    {
      iter = ((JsonArray)val).iter();
    }
    else
    {
      iter = new SingleJsonValueIterator(input);
    }
    return new JsonIterator()
    {
      @Override
      public boolean moveNext() throws Exception
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
    Schema s = SchemaTransformation.wrapIntoArrayOrNull(inputSchema);
    boolean inputMaybeNull =  s.is(NULL).maybe();
    s = SchemaTransformation.removeNullability(s);
    if (s == null)
    {
      return new PathStepSchema(SchemaFactory.nullSchema(), Bool3.TRUE);
    }
    JsonLong minLength = inputSchema.minElements();
    JsonLong maxLength = inputSchema.maxElements();
    PathStepSchema elements = nextStep().getSchema(s.elements());

    Schema result;
    switch(elements.hasData)
    {
    case TRUE:
      result = new ArraySchema(null, elements.schema, minLength, maxLength);
      break;
    case FALSE:
      result = SchemaFactory.emptyArraySchema();
      break;
    default:
      result = SchemaTransformation.merge(
          new ArraySchema(null, elements.schema, minLength, maxLength), 
          SchemaFactory.emptyArraySchema());
    }
    if (inputMaybeNull) {
      result = SchemaTransformation.addNullability(result);
    }
    return new PathStepSchema(result, Bool3.TRUE);
  }
}

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

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;

/** e.g., [0] */
public class PathIndex extends PathStep
{
  /**
   * @param exprs
   */
  public PathIndex(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param index
   */
  public PathIndex(Expr index)
  {
    super(index, new PathReturn());
  }

  /**
   * @param index
   * @param next
   */
  public PathIndex(Expr index, Expr next)
  {
    super(index, next);
  }

  /**
   * 
   * @return
   */
  public final Expr indexExpr()
  {
    return exprs[0];
  }
  
  /**
   * 
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
  throws Exception
  {
    exprText.print("[");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print("]");
    exprs[1].decompile(exprText, capturedVars);
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.PathExpr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonValue eval(Context context) throws Exception
  {
    JsonArray arr = (JsonArray)input;
    if( arr == null )
    {
      return null;
    }
    JsonNumber index = (JsonNumber)indexExpr().eval(context);
    if( index == null )
    {
      return null;
    }
    JsonValue value = arr.get(index.longValueExact());
    return nextStep(context, value);
  }
  
  @Override
  public PathStepSchema getSchema(Schema inputSchema)
  {
    Schema result = null;
    if (indexExpr() instanceof ConstExpr)
    {
      ConstExpr c = (ConstExpr) indexExpr();
      JsonLong i = new JsonLong(((JsonNumber)c.value).longValueExact());
      result = inputSchema.element(i);
      if (result != null && inputSchema.hasElement(i).maybeNot()) 
      {
        result = SchemaTransformation.addNullability(result);
      }
    }
    if (result == null) 
    {
      result = SchemaFactory.anySchema();
    }
    return nextStep().getSchema(result);
  }
}

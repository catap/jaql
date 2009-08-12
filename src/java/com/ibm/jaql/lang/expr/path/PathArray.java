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

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.util.Bool3;
import static com.ibm.jaql.json.type.JsonType.*;

public abstract class PathArray extends PathStep
{
  protected SpilledJsonArray tempArray;

  /**
   * @param exprs
   */
  public PathArray(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr0
   */
  public PathArray(Expr expr0)
  {
    super(expr0);
  }

  /**
   * @param expr0
   * @param expr1
   */
  public PathArray(Expr expr0, Expr expr1)
  {
    super(expr0, expr1);
  }

  /**
   * @param expr0
   * @param expr1
   */
  public PathArray(Expr expr0, Expr expr1, Expr expr2)
  {
    super(expr0, expr1, expr2);
  }


  public PathArray(ArrayList<PathStep> exprs)
  {
    super(exprs);
  }
  
  public Schema getSchema()
  {
    return SchemaFactory.arrayOrNullSchema();
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#iter(com.ibm.jaql.lang.core.Context)
   */
  public abstract JsonIterator iter(Context context) throws Exception;

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonValue eval(Context context) throws Exception
  {
    JsonIterator iter = this.iter(context);
    if (iter.isNull())
    {
      return null;
    }
    if (tempArray == null)
    {
      tempArray = new SpilledJsonArray();
    }
    tempArray.setCopy(iter);
    return tempArray;
  }
  
  protected PathStepSchema getSchemaAll(Schema inputSchema)
  {
    PathStepSchema elements = null;
    boolean inputMaybeNull = false;
    if (inputSchema.is(ARRAY).never())
    {
      // TODO: this indicates a compile-time error but unless error handling is implemented, 
      // we are silent here
      return new PathStepSchema(null, Bool3.FALSE);
    }
    else 
    {
      Schema s = SchemaTransformation.restrictToArrayOrNull(inputSchema);
      if (s==null)  return new PathStepSchema(null, Bool3.FALSE); // TODO: silent here as well
      inputMaybeNull = s.is(NULL).maybe();
      s = SchemaTransformation.removeNullability(s);
      elements = nextStep().getSchema(inputSchema.elements());
    }

    Schema result; 
    if (elements.hasData.always() && !inputMaybeNull)
    {
      result = new ArraySchema(null, elements.schema, null, null);
    }
    else if (elements.hasData.never())
    {
      result = SchemaFactory.emptyArraySchema();
    }
    else
    {
      result = SchemaTransformation.merge(new ArraySchema(null, elements.schema, null, null), 
          SchemaFactory.emptyArraySchema());
    }
      
    return new PathStepSchema(result, Bool3.TRUE);
  }
}


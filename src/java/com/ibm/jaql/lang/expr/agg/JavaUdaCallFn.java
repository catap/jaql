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
package com.ibm.jaql.lang.expr.agg;

import static com.ibm.jaql.lang.expr.core.ExprProperty.HAS_CAPTURES;
import static com.ibm.jaql.lang.expr.core.ExprProperty.HAS_SIDE_EFFECTS;
import static com.ibm.jaql.lang.expr.core.ExprProperty.IS_NONDETERMINISTIC;
import static com.ibm.jaql.lang.expr.core.ExprProperty.READS_EXTERNAL_DATA;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.JsonValueParameter;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.Bool3;


public class JavaUdaCallFn extends AlgebraicAggregate
{
  protected JavaUda aggregate;
  
  // -- construction ------------------------------------------------------------------------------
  
  public JavaUdaCallFn(Expr ... exprs)
  {
    super(exprs);
  }

  
  // -- descriptor --------------------------------------------------------------------------------

  public static class Descriptor extends DefaultBuiltInFunctionDescriptor
  {
    public Descriptor()
    {
      super(
          "javaudacall",
          JavaUdaCallFn.class,
          new JsonValueParameters(
              new JsonValueParameter("array", SchemaFactory.arrayOrNullSchema()),
              new JsonValueParameter("class", SchemaFactory.stringSchema()),
              new JsonValueParameter("args", SchemaFactory.anySchema(), true)),
            SchemaFactory.anySchema());
    }
  }
  
  
  // -- aggregation -------------------------------------------------------------------------------
  
  @Override
  public void init(Context context) throws Exception
  {
    // get an instance of the aggregate
    JsonString className = (JsonString)exprs[1].eval(context);
    aggregate = (JavaUda) Class.forName(className.toString()).newInstance();
    
    // obtain additional parameters
    JsonValue[] initArgs = new JsonValue[exprs.length-2];
    for (int i=0; i<initArgs.length; i++)
    {
      Expr expr = exprs[i+2];
      if (expr.getProperty(HAS_SIDE_EFFECTS, true).maybe()
          || expr.getProperty(IS_NONDETERMINISTIC, true).maybe()
          || expr.getProperty(READS_EXTERNAL_DATA, true).maybe()
          || expr.getProperty(HAS_CAPTURES, true).maybe())
      {
        throw new IllegalArgumentException(
            "argument " + (i+3) + " to aggregate function has to be deterministic and side-effect free"
        );
      }
      initArgs[i] = expr.eval(context);
    }
    
    // initialize the aggregate
    aggregate.init(initArgs);
  }

  @Override
  public void accumulate(JsonValue value) throws Exception
  {
    aggregate.accumulate(value);    
  }

  public void accumulate(JsonIterator iter) throws Exception
  {
    for (JsonValue value : iter) 
    {
      if (value != null)
      {
        accumulate(value);
      }
    }
  }
  
  @Override
  public JsonValue getPartial() throws Exception
  {
    return aggregate.getPartial();
  }
  
  @Override 
  public Schema getPartialSchema()
  {
    try
    {
      if (exprs[1].isCompileTimeComputable().always())
      {
        // get an instance of the aggregate
        JsonString className = (JsonString)exprs[1].compileTimeEval();
        aggregate = (JavaUda) Class.forName(className.toString()).newInstance();
        return aggregate.getPartialSchema();
      }
    } catch (Exception e)
    {
      JaqlUtil.rethrow(e);
    }
    return SchemaFactory.anySchema();    
  }
  
  @Override
  public void combine(JsonValue value) throws Exception
  {
    aggregate.combine(value);
  }
  
  @Override
  public JsonValue getFinal() throws Exception
  {
    return aggregate.getFinal();
  }

  @Override
  public Schema getSchema()
  {
    try
    {
      if (exprs[1].isCompileTimeComputable().always())
      {
        // get an instance of the aggregate
        JsonString className = (JsonString)exprs[1].compileTimeEval();
        aggregate = (JavaUda) Class.forName(className.toString()).newInstance();
        return aggregate.getFinalSchema();
      }
    } catch (Exception e)
    {
      JaqlUtil.rethrow(e);
    }
    return SchemaFactory.anySchema();
  }
  
  // -- evaluation --------------------------------------------------------------------------------

  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    if( i == 0 )
    {
      return Bool3.TRUE;
    }
    return Bool3.FALSE;
  }

  /**
   * This function is called by the AggregateExpr for computing
   * many aggregates simultaneously.  The argument expression
   * is the entire array to aggregate.
   * 
   * The default way to process the input aggregation set is to
   *    for each item in input
   *     - if item is null
   *         - ignore the item
   *     - otherwise add the item
   * 
   * If you override this function, you need to override processInitial as well.
   */
  @Override
  public JsonValue eval(Context context) throws Exception
  {
    init(context);
    accumulate(exprs[0].iter(context));
    return getFinal();
  }
}

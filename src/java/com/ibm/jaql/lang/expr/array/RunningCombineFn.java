/*
 * Copyright (C) IBM Corp. 2010.
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
package com.ibm.jaql.lang.expr.array;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.BuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.expr.function.JsonValueParameter;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;
import com.ibm.jaql.util.Bool3;


public class RunningCombineFn extends IterExpr
{
  public static class Descriptor implements BuiltInFunctionDescriptor 
  {
    private Schema schema = SchemaFactory.arraySchema(); // TODO: add output item from function
    private JsonValueParameters parameters;

    public Descriptor() {
      parameters = new JsonValueParameters(new JsonValueParameter[] {
          new JsonValueParameter("input", SchemaFactory.arrayOrNullSchema()),
          new JsonValueParameter("init", SchemaFactory.anySchema()),
          new JsonValueParameter("add", SchemaFactory.functionSchema()),
          new JsonValueParameter("into", SchemaFactory.functionOrNullSchema()) // todo: make default [state,inval]?
      });
    }

    @Override
    public Expr construct(Expr[] positionalArgs)
    {
      return new RunningCombineFn(positionalArgs);
    }

    @Override
    public Class<? extends Expr> getImplementingClass() 
    {
      return RunningCombineFn.class;
    }

    @Override
    public String getName()
    {
      return "runningCombine";
    }

    @Override
    public JsonValueParameters getParameters()
    {
      return parameters;
    }

    @Override
    public Schema getSchema()
    {
      return schema;
    }
  }

  public RunningCombineFn(Expr... exprs)
  {
    super(exprs);
  }

  public final Expr inputExpr()
  {
    return exprs[0];
  }

  public final Expr initExpr()
  {
    return exprs[1];
  }

  public final Expr addExpr()
  {
    return exprs[2];
  }

  public final Expr intoExpr()
  {
    return exprs[3];
  }

  @Override
  public Schema getSchema()
  {
    return SchemaFactory.arraySchema(); // TODO: fill in from into result
  }

  
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE; // TODO: what is the right answer for functions?
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    final JsonIterator iter = inputExpr().iter(context);
    final JsonValue init = initExpr().eval(context);
    final Function addFn = (Function)addExpr().eval(context);
    final Function intoFn = (Function)intoExpr().eval(context);
    
    // args: [state, inval]
    final JsonValue[] args = new JsonValue[2];    
    args[0] = JsonUtil.getCopy(init, null);
    // We own args[0], but args[1] is not ours

    return new JsonIterator() 
    {
      public boolean moveNext() throws Exception
      {
        if( !iter.moveNext() )
        {
          return false;
        }
        
        // state = add(state,inval)
        // args[0] already set
        args[1] = iter.current();
        addFn.setArguments(args, 0, 2);  // FIXME: this should be done once
        JsonValue newState = addFn.eval(context);
        args[0] = JsonUtil.getCopy(newState, args[0]);

        // yield into(state,inval)
        intoFn.setArguments(args, 0, 2); // FIXME: this should be done once
        currentValue = intoFn.eval(context);

        return true;
      }
    };
  }
}

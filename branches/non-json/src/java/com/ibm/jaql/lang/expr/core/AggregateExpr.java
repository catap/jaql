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
package com.ibm.jaql.lang.expr.core;

import java.io.PrintStream;
import java.util.HashSet;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.json.util.SingleJsonValueIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.agg.Aggregate;
import com.ibm.jaql.lang.expr.agg.AlgebraicAggregate;
import com.ibm.jaql.util.Bool3;


public abstract class AggregateExpr extends IterExpr // TODO: add init/combine/final flags
{
  protected JsonValue[] tempAggs;
  protected BufferedJsonArray tmpArray;

  public static enum AggType
  {
    INITIAL("initial"),
    PARITIAL("partial"),
    FINAL("final"),
    FULL("full");

    private final String name;
    private AggType(String name) { this.name = name; }
    public String toString() { return name; };
  }
  
  // Binding input, Aggregate[] aggregates 
  public AggregateExpr(Expr[] inputs)
  {
    super(inputs);
  }
  
  public abstract AggType getAggType();

  public final BindingExpr binding()
  {
    return (BindingExpr)exprs[0];
  }

  public final int numAggs()
  {
    return exprs.length - 1;
  }

  public final Aggregate agg(int i)
  {
    return (Aggregate)exprs[i+1];
  }
  
  public Schema getSchema()
  {
    return SchemaFactory.arraySchema();
  }

  /**
   * 
   */
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
   * @return true iff all aggregates are algebraic.
   */
  public boolean isAlgebraic()
  {
    for(int i = 1 ; i < exprs.length ; i++)
    {
      if( !(exprs[i] instanceof AlgebraicAggregate) )
      {
        return false;
      }
    }
    return true;
  }

  protected void onlyTrivialInput()
  {
    int n = numAggs();
    for(int i = 0 ; i < n ; i++)
    {
      Aggregate a = (Aggregate)agg(i);
      Var asVar = binding().var;
      Expr c = a.child(0);
      if( !(c instanceof VarExpr) || ((VarExpr)c).var() != asVar )
      {
        throw new RuntimeException("only 'as' variable is allowed inside aggFn($as) of aggregate "+getAggType());
      }
    }

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars) // TODO: get rid of captured vars during decompile
      throws Exception
  {
    // input -> aggregate (each var)? expr
    final BindingExpr in = binding();
    in.inExpr().decompile(exprText, capturedVars);
    exprText.print("\n-> aggregate as ");
    exprText.print(in.var.name());
    exprText.print(" ");
    exprText.print(getAggType());
    exprText.print(" [");
    String sep = " ";
    int n = numAggs();
    for(int i = 0 ; i < n ; i++)
    {
      exprText.print(sep);
      Aggregate agg = agg(i);
      agg.decompile(exprText, capturedVars);
      sep = ", ";
    }
    exprText.print("]");
    capturedVars.remove(in.var);
  }

  protected void makeWorkingArea()
  {
    tempAggs = new JsonValue[numAggs()];
    tmpArray = new BufferedJsonArray(1);
  }
  
  /**
   * 
   * @param context
   * @param aggs
   * @param iter
   * @return true if we had at least one input row
   * @throws Exception
   */
  public boolean evalInitial(Context context, Aggregate[] aggs)
    throws Exception
  {
    for(int i = 0 ; i < aggs.length ; i++)
    {
      aggs[i].init(context);
    }

    boolean hadInput = false;
    BindingExpr in = binding();    
    JsonIterator iter = in.inExpr().iter(context);
    for (JsonValue value : iter)
    {
      hadInput = true;
      tmpArray.set(0, value);
      in.var.setValue(tmpArray);
      for(int i = 0 ; i < aggs.length ; i++)
      {
        aggs[i].evalInitialized(context);
      }
    }
    
    return hadInput;
  }

  
  protected JsonIterator finalResult(boolean hadInput, Aggregate[] aggs) throws Exception
  {
    if( ! hadInput )
    {
      return JsonIterator.EMPTY;
    }
    for(int i = 0 ; i < aggs.length ; i++)
    {
      tempAggs[i] = aggs[i].getFinal();
    }
    BufferedJsonArray tuple = new BufferedJsonArray(tempAggs, false); // TODO: memory
    return new SingleJsonValueIterator(tuple); // TODO: memory
  }

}

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

import java.util.ArrayList;
import java.util.HashSet;

import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.agg.Aggregate;
import com.ibm.jaql.lang.expr.array.AsArrayFn;
import com.ibm.jaql.lang.expr.array.ToArrayFn;
import com.ibm.jaql.lang.expr.nil.EmptyOnNullFn;


public class AggregateFullExpr extends AggregateExpr
{
  protected Aggregate[] aggs;
  
  protected static Expr[] makeArgs(BindingExpr input, ArrayList<Expr> aggs)
  {
    int n = aggs.size();
    Expr[] args = new Expr[n+1];
    args[0] = input;
    for(int i = 0 ; i < n ; i++)
    {
      args[i+1] = aggs.get(i);
    }
    return args;
  }

  /** Find the aggregates of a pipeline over aggVar.
   * This is expected to be part of expression like this:
   * 
   *     aggregate as _aggVar_ into _into_
   * 
   * @param aggVar the aggregate variable
   * @param into the aggregation expression to search
   * @param varUses returns all the uses of aggVar 
   * @param aggs returns the aggregates found (possibly empty)
   * @param raiseException if we cannot find the aggregates 
   *        raise an exception on true else return false.
   * @return true if all uses of aggVar pipeline into aggregates
   */
  private static boolean findAggregates(
      Var aggVar,
      Expr into, 
      ArrayList<Expr> varUses,
      ArrayList<Expr> aggs,
      boolean raiseException)
  {
    varUses.clear();
    aggs.clear();
    Expr top = into.parent();
    HashSet<Var> safeVars = into.getCapturedVars();
    into.getVarUses(aggVar, varUses);
    for( Expr e: varUses )
    {
      assert e instanceof VarExpr;
      Expr p = e.parent();
      
      // Search up through mappable expressions to an Aggregate.
      while( p != top && !(p instanceof Aggregate) )
      {
        if( p instanceof ToArrayFn || 
            p instanceof AsArrayFn ||
            p instanceof EmptyOnNullFn )
        {
          // The aggVar is always an array or null in co-group.
          // Every aggregate and mappable expression treats null as empty array.
          // So it's safe to move over these.
          assert p.numChildren() == 1;
        }
        else
        {
          if( p instanceof BindingExpr )
          {
            // Skip over BindingExpr to it's proper parent expr
            // It's parent should never be null in a healthy expr tree
            assert e.getChildSlot() == 0;
            e = p;
            p = e.parent();
          }
          if( ! p.isMappable(e.getChildSlot()) )
          {
            // We found a use of the aggVar that isn't mapping into an aggregate.
            return failed(raiseException, p, 
                "use of aggregation variable is not mapping into an aggregate function");
          }
        }
        e = p;
        p = e.parent();
      }
      
      if( p instanceof Aggregate )
      {
        // If aggVar is not be headed into the aggregate as the primary data input
        // then we cannot use AggregateFull.
        if( e.getChildSlot() != 0 ) 
        {
          return failed(raiseException, p, 
              "use of aggregation variable is not mapping into the primary input of an aggregate function");
        }
        
        // If that the aggregates are using any variables defined
        // within the into expr, then we cannot use AggregateFull.
        // Variables from outside the into expr including aggVar are safe.
        HashSet<Var> aggCaptures = p.getCapturedVars();
        aggCaptures.removeAll(safeVars);
        if( ! aggCaptures.isEmpty() )
        {
          return failed(raiseException, p, 
              "aggregation cannot reference local variables "+aggCaptures);
        }
        
        // This aggregate is good for AggregateFull
        aggs.add((Aggregate)p);
      }
      else if( p == top )
      {
        // We found a use of the aggVar that isn't headed into an aggregate at all.
        return failed(raiseException, p, 
            "use of aggregation variable that is not aggregating");
      }
    }
    
    // If we are rewriting (vs a direct call) and there are no aggregates
    // then we don't rewrite.  We could, but then we will fight with LetInline
    // which undoes what InjectAggregate does.
    if( ! raiseException && aggs.size() == 0 )
    {
      return false;
    }
    
    return true;
  }

  private static boolean failed(boolean raiseException, Expr p, String message)
  {
    if( raiseException )
    {
      throw new RuntimeException(message + ":\n"+ p.toString());
    }
    return false;
  }

  /**
   * Make an expression involving AggregateFullExpr if possible or return null if not.
   * 
   * If null is returned the tree is not modified and should be returned as quickly as possible
   * because this method is used during rewrite.
   * 
   * @param env The environment to create variables.
   * @param aggVar The variable bound
   * @param input Defines the array to aggregate. When null, use VarExpr(new aggVar).
   * @param into The expression that aggregates the input.
   * @param varUses A worklist for our use.
   * @param aggs Another worklist for our use.
   * @return An aggregating expression or null.
   */
  public static Expr makeIfAggregating(
      Env env, 
      Var aggVar, 
      Expr input, 
      Expr into,
      ArrayList<Expr> varUses,
      ArrayList<Expr> aggs,
      boolean raiseExceptionIfNotPossible)
  {
    if( ! findAggregates(aggVar, into, varUses, aggs, raiseExceptionIfNotPossible) ) 
    {
      assert !raiseExceptionIfNotPossible;
      return null;
    }
    
    // If the into expression is already in canonical form, just return it unmodified.
    boolean isCanonical = false;
    if( into instanceof ArrayExpr && 
        aggs.size() == into.numChildren() )
    {
      isCanonical = true;
      for( int i = 0 ; i < aggs.size() ; i++ )
      {
        if( aggs.get(i) != into.child(i) )
        {
          isCanonical = false;
          break;
        }
      }
    }

    if( input == null )
    {
      // Only when called during InjectAggregate is input null.
      // aggVar is the data to aggregate, but the var belongs to someone else (probably a group by).
      // We make input = aggVar, make a new var for the AggregateFullExpr, and replace all the
      // uses of the old aggVar with the new aggVar.
      input = new VarExpr(aggVar);
      aggVar = env.makeVar("$toagg");
      for( Expr e: varUses )
      {
        ((VarExpr)e).setVar(aggVar);
      }
    }
    BindingExpr binding = new BindingExpr(BindingExpr.Type.EQ, aggVar, null, input);
    
    if( isCanonical )
    {
      // Just return this:
      // input -> aggregate as aggVar [ agg0(...aggVar...), ..., aggn(...aggVar...) ],
      return new AggregateFullExpr(binding, aggs);
    }

    // Make into canonical form:
    //
    //    ( $aggs = input -> aggregate as aggVar [ agg0(...aggVar...), ..., aggn(...aggVar...) ],
    //      into(...$aggs[0]...$aggs[n]...) )

    // Replace all aggs in into by $aggs[i] 
    Var tempVar = env.makeVar("$aggs", SchemaFactory.arraySchema());
    int i = 0;
    for( Expr agg: aggs )
    {
      Expr e = new IndexExpr(new VarExpr(tempVar), i++);
      if( agg == into )
      {
        into = e;
      }
      else
      {
        agg.replaceInParent(e);
      }
    }

    // Add block
    Expr expr = new AggregateFullExpr(binding, aggs);
    expr = new BindingExpr(BindingExpr.Type.IN, tempVar, null, expr);
    expr = new DoExpr(expr, into);
    
    return expr;
  }
    
  public static Expr make(Env env, BindingExpr input, Expr into)
  {
    ArrayList<Expr> varUses = new ArrayList<Expr>();
    ArrayList<Expr> aggs = new ArrayList<Expr>();
    Expr expr = makeIfAggregating(env, input.var, input.eqExpr(), into, varUses, aggs, true);
    assert expr != null;
    return expr;
  }
  
//  private static Expr splitExpr1(Var aggVar, Var outVar, Expr expr, ArrayList<Aggregate> aggs)
//  {
//    if( expr instanceof VarExpr )
//    {
//      VarExpr ve = (VarExpr)expr;
//      if( ve.var == aggVar )
//      {
//        throw new RuntimeException("the aggregation variable must be inside an aggregate");
//      }
//      return expr;
//    }
//    
//    if ( expr instanceof FunctionCallExpr )
//    {
//      // force inline of calls to aggregate functions
//      FunctionCallExpr call = (FunctionCallExpr)expr;
//      if (call.fnExpr().isCompileTimeComputable().always())
//      {
//        try
//        {
//          Function ff = (Function)call.fnExpr().compileTimeEval();
//          if (ff instanceof BuiltInFunction)
//          {
//            BuiltInFunction f = (BuiltInFunction)ff;
//            if (Aggregate.class.isAssignableFrom(f.getDescriptor().getImplementingClass()))
//            {
//              Expr inline = call.inline();
//              expr.replaceInParent(inline);
//              expr = inline;
//            }
//          }
//        } catch (Exception e1)
//        {
//          // ignore
//        }
//      }
//    }
//    
//    if( expr instanceof Aggregate )
//    {
//      Aggregate agg = (Aggregate)expr;
//      int i = aggs.size();
//      aggs.add(agg);
//      Expr e = new IndexExpr(new VarExpr(outVar), i);
//      if( agg.parent() == null )
//      {
//        return e;
//      }
//      else
//      {
//        agg.replaceInParent(e);
//      }
//      return expr;
//    }
//
//    for( Expr e: expr.exprs )
//    {
//      splitExpr(aggVar, outVar, e, aggs);
//    }
//    return expr;
//  }

//  /**
//   * Return a new canonical aggregate expression, which might have an MapExpr on top of it.
//   * 
//   * @param env
//   * @param aggVar
//   * @param input
//   * @param expr
//   * @param expand True if expanding expr.
//   * @return
//   */
//  public static Expr make(Env env, BindingExpr input, Expr expr, boolean expand)
//  {
//    if( !expand && expr instanceof ArrayExpr )
//    {
//      // Don't add map if we are already canonical: aggregate [ agg1(..), ..., aggN(...) ]
//      boolean allAggs = true;
//      for( Expr e: expr.exprs )
//      {
//        if( ! (e instanceof Aggregate) )
//        {
//          allAggs = false;
//          break;
//        }
//      }
//      if( allAggs )
//      {
//        int n = expr.numChildren();
//        Expr[] exprs = new Expr[n + 1];
//        exprs[0] = input;
//        System.arraycopy(expr.exprs, 0, exprs, 1, n);
//        return new AggregateFullExpr(exprs);
//      }
//    }
//    Var outVar = env.makeVar("$");
//    ArrayList<Aggregate> aggs = new ArrayList<Aggregate>();
//    expr = splitExpr(input.var, outVar, expr, aggs);
//    Expr e = new AggregateFullExpr(input, aggs);
//    if( expand )
//    {
//      e = new ForExpr(outVar, e, expr);
//    }
//    else
//    {
//      e = new TransformExpr(outVar, e, expr);
//    }
//    return e;
//  }

  
  // Binding input, Aggregate[] aggregates 
  public AggregateFullExpr(Expr... inputs)
  {
    super(inputs);
  }
  
  /**
   * @param binding
   * @param aggs A list of Aggregates
   */
  public AggregateFullExpr(BindingExpr binding, ArrayList<Expr> aggs)
  {
    super(makeArgs(binding, aggs));
  }
  
  public AggType getAggType()
  {
    return AggType.FULL;
  }

  protected void makeWorkingArea()
  {
    if( aggs == null )
    {
      super.makeWorkingArea();
      int n = numAggs();
      aggs = new Aggregate[n];
      for(int i = 0 ; i < n ; i++)
      {
        aggs[i] = agg(i);
      }
    }
    else
    {
      assert numAggs() == aggs.length && aggs.length == tempAggs.length;
    }
  }

  @Override
  public JsonArray eval(final Context context) throws Exception
  {
    makeWorkingArea();
    evalInitial(context, aggs);
    return finalResult(aggs);
  }
}

/*
 * Copyright (C) IBM Corp. 2008.
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

import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.lang.expr.core.MathExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.nil.FirstNonNullFn;

/**
 * 
 */
@JaqlFn(fnName = "count", minArgs = 1, maxArgs = 1)
public class CountAgg extends AlgebraicAggregate
{
  /**
   * count(array)
   * 
   * @param exprs
   */
  public CountAgg(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param exprs
   */
  public CountAgg(Expr exprs)
  {
    super(new Expr[]{exprs});
  }

  //@Override
  //protected DistributiveAggregate aggExpr(Expr initLoop) throws Exception
  //{
  //  return new SumExpr(initLoop);
  //}

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.agg.AlgebraicAggregate#initExpr(com.ibm.jaql.lang.core.Var)
   */
  @Override
  protected Expr initExpr(Var var) throws Exception
  {
    return new ConstExpr(JLong.ONE_ITEM);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.agg.AlgebraicAggregate#combineExpr(com.ibm.jaql.lang.core.Var,
   *      com.ibm.jaql.lang.core.Var)
   */
  @Override
  protected Expr combineExpr(Var var1, Var var2) throws Exception
  {
    return new MathExpr(MathExpr.PLUS, new VarExpr(var1), new VarExpr(var2));
  }

  //  @Override
  //  protected Expr emptyExpr() throws Exception
  //  {
  //    return new ConstExpr(JLong.ZERO_ITEM);
  //  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.agg.AlgebraicAggregate#finalExpr(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  protected Expr finalExpr(Expr agg) throws Exception
  {
    return new FirstNonNullFn(agg, new ConstExpr(JLong.ZERO_ITEM));
    // return agg;
  }

  /*
   * public Item eval(final Context context) throws Exception { long n = 0; if(
   * exprs[0] instanceof IterExpr ) { Iter iter = exprs[0].iter(context); if(
   * iter.isNull() ) { return Item.nil; } while( iter.next() != null ) { n++; } }
   * else { JArray arr = (JArray)exprs[0].eval(context).get(); if( arr == null ) {
   * return Item.nil; } n = arr.count(); } return new Item(new LongItem(n)); //
   * TODO: cache }
   */
}

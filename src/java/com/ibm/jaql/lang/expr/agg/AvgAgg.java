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

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.MathExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * 
 */
public class AvgAgg extends AlgebraicAggregate
{
  private SumAgg.Summer summer = new SumAgg.Summer();
  private long count = 0;
  
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("avg", AvgAgg.class);
    }
  }
  
  /**
   * one arg
   * @param exprs
   */
  public AvgAgg(Expr[] exprs)
  {
    super(exprs);
  }

  public AvgAgg(Expr arg)
  {
    super(arg);
  }
  
  @Override
  public void init(Context context) throws Exception
  {
    summer.init();
    count = 0;
  }

  @Override
  public void accumulate(JsonValue value) throws Exception
  {
    if( value == null  )
    {
      return;
    }
    summer.add(value);
    count++;
  }

  @Override
  public JsonValue getPartial() throws Exception
  {
    JsonValue sum = summer.get();
    JsonValue cnt = new JsonLong(count);
    BufferedJsonArray pair = new BufferedJsonArray(new JsonValue[]{sum,cnt}, false);
    return pair;
  }

  @Override
  public void combine(JsonValue value) throws Exception
  {
    JsonArray arr = (JsonArray)value;
    JsonValue[] pair = new JsonValue[2];
    arr.getAll(pair);
    summer.add(pair[0]);
    JsonLong jlong = (JsonLong)pair[1];
    count += jlong.get();
  }

  @Override
  public Schema getPartialSchema()
  {
    Schema in = exprs[0].getSchema();
    return new ArraySchema(new Schema[] { 
        SumAgg.Summer.getSchema(in), 
        SchemaFactory.longSchema() });
  }

  @Override
  public Schema getSchema()
  {
    Schema in = exprs[0].getSchema();
    return SumAgg.Summer.getSchema(in);    // division does not promote
  }
  
  @Override
  public JsonValue getFinal() throws Exception
  {
    JsonValue sum = summer.get();
    JsonLong n = new JsonLong(count);
    return MathExpr.eval(sum, n, MathExpr.DIVIDE);    
  }

  
//  /*
//   * (non-Javadoc)
//   * 
//   * @see com.ibm.jaql.lang.expr.core.MacroExpr#expand(com.ibm.jaql.lang.core.Env)
//   */
//  @Override
//  public Expr expand(Env env) throws Exception
//  {
//    // TODO: expand should be done by a transformation
//    return new SumAgg(exprs[0].clone(varMap)
//    Var var = env.makeVar("$avgIn");
//    Expr sum = new SumAgg(new VarExpr(var)).expand(env);
//    Expr count = new CountAgg(new VarExpr(var)).expand(env);;
//    Expr divide = new MathExpr(MathExpr.DIVIDE, sum, count);
//    Expr denull = new DenullFn(exprs[0]).expand(env);
//    LetExpr letExpr = new LetExpr(var, denull, divide);
//    return letExpr;
//  }
//
//  /*
//   * @Override protected Expr initExpr(Env env, Var forVar) throws Exception {
//   * return new IfExpr( new IsnullExpr(new VarExpr(forVar)), new ListExpr( new
//   * ConstExpr(LongItem.ZERO_ITEM), new ConstExpr(LongItem.ZERO_ITEM)), new
//   * ListExpr( new VarExpr(forVar), new ConstExpr(LongItem.ONE_ITEM))); }
//   * 
//   * @Override protected DistributiveAggregate aggExpr(Env env, Expr initLoop)
//   * throws Exception { return new VectorSum(initLoop); }
//   * 
//   * @Override protected Expr finalExpr(Env env, Expr agg) throws Exception {
//   * return agg; }
//   */
//
//  //  public Item eval(final Context context) throws Exception
//  //  {
//  //    boolean sawLong = false;
//  //    long count = 0;
//  //    long lsum = 0;
//  //    BigDecimal sum = null;
//  //    Iter iter = exprs[0].iter(context);
//  //    if( iter.isNull() )
//  //    {
//  //      return Item.nil;
//  //    }
//  //    Item item;
//  //    while( (item = iter.next()) != null )
//  //    {
//  //      JaqlType w = item.get();
//  //      if( w == null) 
//  //      {
//  //        continue;
//  //      }
//  //      else if( w instanceof LongItem )
//  //      {
//  //        sawLong = true;
//  //        lsum += ((LongItem)w).value;
//  //        count++;
//  //      }
//  //      else
//  //      {
//  //        DecimalItem n = (DecimalItem)w;
//  //        // TODO: need a mutable BigDecimal...
//  //        if( sum == null )
//  //        {
//  //          sum = n.value;
//  //        }
//  //        else
//  //        {
//  //          sum = sum.add(n.value);
//  //        }
//  //        count++;
//  //      }
//  //    }
//  //    if( sum == null )
//  //    {
//  //      if( sawLong )
//  //      {
//  //        return new Item(new LongItem(lsum/count));  // TODO: memory
//  //      }
//  //      return Item.nil;
//  //    }
//  //    else
//  //    {
//  //      if( lsum != 0 )
//  //      {
//  //        sum = sum.add(new BigDecimal(lsum));
//  //      }
//  //      return new Item(new DecimalItem(sum.longValue()/count)); // TODO: memory
//  //    }
//  //  }
}

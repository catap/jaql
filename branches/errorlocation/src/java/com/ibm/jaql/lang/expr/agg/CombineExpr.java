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

import java.util.Arrays;

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * 
 */
public final class CombineExpr extends AlgebraicAggregate // extends PushAggExpr // TODO: kill PushAggExpr code
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("combine", CombineExpr.class);
    }
  }
  
  /**
   * BindingExpr in, Expr using, Expr init
   * 
   * @param exprs
   */
  public CombineExpr(Expr[] exprs)
  {
    super(exprs);
  }

//  /**
//   * combine $i,$j in inExpr using usingExpr($i,$j)
//   * 
//   * @param var1
//   * @param var2
//   * @param inExpr
//   * @param usingExpr
//   */
//  public CombineExpr(Var var1, Var var2, Expr inExpr, Expr usingExpr)
//  {
//    super(
//        new Expr[]{
//            new BindingExpr(BindingExpr.Type.INPAIR, var1, var2, inExpr),
//            usingExpr});
//  }
//
//  //  public CombineExpr(Var var1, Var var2, Expr inExpr, Expr usingExpr, Expr emptyExpr)
//  //  {
//  //    super(new Expr[]{ 
//  //        new BindingExpr(BindingExpr.Type.INPAIR, var1, var2, inExpr), 
//  //        usingExpr,
//  //        emptyExpr == null ? new ConstExpr(Item.nil) : emptyExpr
//  //    });
//  //  }

//  /**
//   * @return
//   */
//  public final BindingExpr binding()
//  {
//    return (BindingExpr) exprs[0];
//  }

//  /**
//   * @return
//   */
//  public final Expr usingExpr()
//  {
//    return exprs[1];
//  }

//  /*
//   * (non-Javadoc)
//   * 
//   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
//   *      java.util.HashSet)
//   */
//  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
//      throws Exception
//  {
//    BindingExpr b = binding();
//    exprText.print("combine( ");
//    exprText.print(b.var.name);
//    exprText.print(",");
//    exprText.print(b.var2.name);
//    exprText.print(" in ");
//    binding().inExpr().decompile(exprText, capturedVars);
//    exprText.print(") ");
//    usingExpr().decompile(exprText, capturedVars);
//    capturedVars.remove(b.var);
//    capturedVars.remove(b.var2);
//    //    if( !( exprs[2] instanceof ConstExpr && 
//    //           ((ConstExpr)exprs[2]).value.get() == null) )
//    //    {
//    //      exprText.print(" when empty (");
//    //      emptyExpr().decompile(exprText, capturedVars);
//    //      exprText.print(")");
//    //    }
//  }

  // Not safe for recursion.
  protected final JsonValue[] agg = new JsonValue[2];
  protected Context context;
  protected int bufIdx = 0;
  protected Function combiner;
  protected final JsonValue[] args = new JsonValue[2];

      
  @Override
  public void init(Context context) throws Exception
  {
    this.context = context;
    bufIdx = 0;
    combiner = JaqlUtil.enforceNonNull((Function)exprs[1].eval(context));
    Arrays.fill(agg, null);
    // context.setVar(binding().var2, agg[0]);
  }

  @Override
  public void accumulate(JsonValue value) throws Exception
  {
    if( value == null )
    {
      return;
    }
    // BindingExpr b = binding();
    JsonValue combined;
    if (agg[bufIdx] == null )
    {
      combined = value;
    }
    else
    {
      args[1] = value;
      combiner.setArguments(args);
      combined = combiner.eval(context);
      // context.setVar(b.var, item);
      // combined = usingExpr().eval(context);
      if (combined == null )
      {
        throw new RuntimeException("combiners cannot return null");
      }
    }
    // We need to use two buffers because we might copy part 
    // of the previous result into the new result.
    bufIdx = 1 - bufIdx; 
    agg[bufIdx] = combined.getCopy(agg[bufIdx]);
    args[0] = agg[bufIdx];
    // context.setVar(b.var2, agg[bufIdx]);
  }

  @Override
  public JsonValue getPartial() throws Exception
  {
    return agg[bufIdx];
  }

  @Override
  public void combine(JsonValue value) throws Exception
  {
    accumulate(value);
  }

  @Override
  public JsonValue getFinal() throws Exception
  {
    return agg[bufIdx];
  }

  // remove all nulls from the input
  // if input is empty, return null
  // repeat until input has one item
  //   pick x,y: any two (non-null) values from input
  //   z = using(x,y)
  //   assert z != null
  //   put z into the input
  //  protected Item evalRaw(final Context context) throws Exception
  //  {
  //    BindingExpr b = (BindingExpr)exprs[0];
  //    Iter iter = b.child(0).iter(context);
  //    if( iter.isNull() )
  //    {
  //      return Item.nil;
  //    }
  //    
  //    Item agg = new Item(); // TODO: memory
  //    context.setVar(b.var, agg);
  //    // agg.copy(exprs[2].eval(context)); // copy the initial value
  //    
  //    Item item;
  //    while( (item = iter.next()) != null )
  //    {
  //      if( ! item.isNull() )
  //      {
  //        Item combined;
  //        if( agg.isNull() )
  //        {
  //          combined = item;
  //        }
  //        else
  //        {
  //          context.setVar(b.var2, item);
  //          combined = exprs[1].eval(context);
  //          if( combined.isNull() )
  //          {
  //            throw new RuntimeException("combiners cannot return null");
  //          }
  //        }
  //        agg.copy(combined);
  //      }
  //    }
  //    return agg;
  //  }

//  /*
//   * (non-Javadoc)
//   * 
//   * @see com.ibm.jaql.lang.expr.agg.PushAggExpr#init(com.ibm.jaql.lang.core.Context)
//   */
//  @Override
//  public PushAgg init(final Context context) throws Exception
//  {
//    final BindingExpr binding = (BindingExpr) exprs[0];
//    final Item[] agg = new Item[] {new Item(), new Item()};
//    //final Item agg = new Item(); // TODO: memory
//    context.setVar(binding.var, agg[0]);
//    final Var var2 = binding.var2;
//
//    final Expr input = binding.inExpr();
//
//    return new PushAgg() {
//      @Override
//      public void addMore() throws Exception
//      {
//        Iter iter = input.iter(context);
//        Item item;
//        int bufIdx = 0;
//        while ((item = iter.next()) != null)
//        {
//          if (!item.isNull())
//          {
//            Item combined;
//            if (agg[bufIdx].isNull())
//            {
//              combined = item;
//            }
//            else
//            {
//              context.setVar(var2, item);
//              combined = exprs[1].eval(context);
//              if (combined.isNull())
//              {
//                throw new RuntimeException("combiners cannot return null");
//              }
//            }
//            bufIdx = (bufIdx+1) %2;
//            agg[bufIdx].copy(combined);
//            context.setVar(binding.var, agg[bufIdx]);
//          }
//        }
//      }
//
//      @Override
//      public Item eval() throws Exception
//      {
//        //return agg;
//        return context.getValue(binding.var);
//      }
//    };
//  }
}

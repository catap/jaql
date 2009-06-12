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
package com.ibm.jaql.lang.core;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;

import com.ibm.jaql.lang.expr.agg.AvgAgg;
import com.ibm.jaql.lang.expr.agg.CountAgg;
import com.ibm.jaql.lang.expr.agg.MaxAgg;
import com.ibm.jaql.lang.expr.agg.MinAgg;
import com.ibm.jaql.lang.expr.agg.MultiAgg;
import com.ibm.jaql.lang.expr.agg.SumAgg;
import com.ibm.jaql.lang.expr.agg.SumPA;
import com.ibm.jaql.lang.expr.array.AppendFn;
import com.ibm.jaql.lang.expr.array.AsArrayFn;
import com.ibm.jaql.lang.expr.array.DeemptyFn;
import com.ibm.jaql.lang.expr.array.DistinctFn;
import com.ibm.jaql.lang.expr.array.EnumerateExpr;
import com.ibm.jaql.lang.expr.array.ExistsFn;
import com.ibm.jaql.lang.expr.array.PairFn;
import com.ibm.jaql.lang.expr.array.PairwiseFn;
import com.ibm.jaql.lang.expr.array.RemoveElementFn;
import com.ibm.jaql.lang.expr.array.ReplaceElementFn;
import com.ibm.jaql.lang.expr.core.CompareFn;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.lang.expr.core.MacroExpr;
import com.ibm.jaql.lang.expr.core.TypeofExpr;
import com.ibm.jaql.lang.expr.date.NowFn;
import com.ibm.jaql.lang.expr.db.JdbcExpr;
import com.ibm.jaql.lang.expr.hadoop.MRAggregate;
import com.ibm.jaql.lang.expr.hadoop.MapReduceFn;
import com.ibm.jaql.lang.expr.hadoop.ReadConfExpr;
import com.ibm.jaql.lang.expr.io.ArrayReadExpr;
import com.ibm.jaql.lang.expr.io.HBaseDeleteExpr;
import com.ibm.jaql.lang.expr.io.HBaseFetchExpr;
import com.ibm.jaql.lang.expr.io.HBaseReadExpr;
import com.ibm.jaql.lang.expr.io.HBaseShellExpr;
import com.ibm.jaql.lang.expr.io.HBaseWriteExpr;
import com.ibm.jaql.lang.expr.io.HadoopTempExpr;
import com.ibm.jaql.lang.expr.io.HdfsReadExpr;
import com.ibm.jaql.lang.expr.io.HdfsShellExpr;
import com.ibm.jaql.lang.expr.io.HdfsWriteExpr;
import com.ibm.jaql.lang.expr.io.HttpGetExpr;
import com.ibm.jaql.lang.expr.io.LocalReadExpr;
import com.ibm.jaql.lang.expr.io.LocalWriteExpr;
import com.ibm.jaql.lang.expr.io.ReadAdapterRegistryExpr;
import com.ibm.jaql.lang.expr.io.ReadExpr;
import com.ibm.jaql.lang.expr.io.RegisterAdapterExpr;
import com.ibm.jaql.lang.expr.io.StReadExpr;
import com.ibm.jaql.lang.expr.io.StWriteExpr;
import com.ibm.jaql.lang.expr.io.UnregisterAdapterExpr;
import com.ibm.jaql.lang.expr.io.WriteAdapterRegistryExpr;
import com.ibm.jaql.lang.expr.io.WriteExpr;
import com.ibm.jaql.lang.expr.net.JaqlGetFn;
import com.ibm.jaql.lang.expr.nil.DenullFn;
import com.ibm.jaql.lang.expr.nil.EmptyOnNullFn;
import com.ibm.jaql.lang.expr.nil.FirstNonNullFn;
import com.ibm.jaql.lang.expr.nil.IsnullFn;
import com.ibm.jaql.lang.expr.nil.NullElementOnEmptyFn;
import com.ibm.jaql.lang.expr.nil.NullOnEmptyFn;
import com.ibm.jaql.lang.expr.numeric.AbsFn;
import com.ibm.jaql.lang.expr.numeric.DoubleFn;
import com.ibm.jaql.lang.expr.numeric.ExpFn;
import com.ibm.jaql.lang.expr.numeric.IntFn;
import com.ibm.jaql.lang.expr.numeric.LnFn;
import com.ibm.jaql.lang.expr.numeric.ModFn;
import com.ibm.jaql.lang.expr.numeric.NumberFn;
import com.ibm.jaql.lang.expr.numeric.ToNumberFn;
import com.ibm.jaql.lang.expr.pragma.ConstPragma;
import com.ibm.jaql.lang.expr.pragma.InlinePragma;
import com.ibm.jaql.lang.expr.random.RandomLongFn;
import com.ibm.jaql.lang.expr.random.RegisterRNGExpr;
import com.ibm.jaql.lang.expr.random.SampleRNGExpr;
import com.ibm.jaql.lang.expr.record.ArityFn;
import com.ibm.jaql.lang.expr.record.FieldsFn;
import com.ibm.jaql.lang.expr.record.HasFieldFn;
import com.ibm.jaql.lang.expr.record.NamesFn;
import com.ibm.jaql.lang.expr.record.RecordFn;
import com.ibm.jaql.lang.expr.record.RemapFn;
import com.ibm.jaql.lang.expr.record.RemoveFieldsFn;
import com.ibm.jaql.lang.expr.record.RenameFieldsFn;
import com.ibm.jaql.lang.expr.record.ValuesFn;
import com.ibm.jaql.lang.expr.regex.RegexFn;
import com.ibm.jaql.lang.expr.regex.RegexMatchFn;
import com.ibm.jaql.lang.expr.regex.RegexSpansFn;
import com.ibm.jaql.lang.expr.regex.RegexTestFn;
import com.ibm.jaql.lang.expr.span.SpanContainsFn;
import com.ibm.jaql.lang.expr.span.SpanFn;
import com.ibm.jaql.lang.expr.span.SpanOverlapsFn;
import com.ibm.jaql.lang.expr.span.TokenizeFn;
import com.ibm.jaql.lang.expr.string.SerializeFn;
import com.ibm.jaql.lang.expr.string.StartsWithFn;
import com.ibm.jaql.lang.expr.string.SubstringFn;
import com.ibm.jaql.lang.expr.udf.JavaFnExpr;
import com.ibm.jaql.lang.registry.ReadFunctionRegistryExpr;
import com.ibm.jaql.lang.registry.RegisterFunctionExpr;
import com.ibm.jaql.lang.registry.WriteFunctionRegistryExpr;

/**
 * 
 */
public class FunctionLib
{
  // private static HashMap<String, FnDecl> lib = new HashMap<String, FnDecl>();
  private static HashMap<String, Class<?>> lib = new HashMap<String, Class<?>>();

  static void add(Class<? extends Expr> cls)
  {
    JaqlFn fn = cls.getAnnotation(JaqlFn.class);
    if (fn != null)
    {
      lib.put(fn.fnName(), cls);
    }
    else
    {
      System.err.println("class is not annotated as a function: "
          + cls.getCanonicalName());
    }
  }

  /**
   * @param fnName
   * @param cls
   */
  public static void add(String fnName, Class<?> cls)
  {
    assert cls.getAnnotation(JaqlFn.class) == null; // builtins should use single arg add() method
    lib.put(fnName, cls);
  }

  static
  {
    // TODO: add "import extension" that loads the functions in some jar (and loads types?)
    add(TypeofExpr.class);
    add(CompareFn.class);
    add(ExistsFn.class);
    //lib.put("loadXml", LoadXmlExpr.class);
    //lib.put("deepCompare", DeepCompareExpr.class);
    add(NowFn.class);
    add(CountAgg.class);
    add(SumAgg.class);
    add(MinAgg.class);
    add(MaxAgg.class);
    add(AvgAgg.class);
    add(SumPA.class);
    add(MultiAgg.class);
    add(ModFn.class);
    add(AbsFn.class);
    add(IntFn.class);
    add(NumberFn.class);
    add(DoubleFn.class);
    add(ToNumberFn.class);
    add(MapReduceFn.class);
    add(MRAggregate.class);
    //    add(MRCogroup.class);
    add(JdbcExpr.class);
    add(SpanFn.class);
    add(SpanOverlapsFn.class);
    add(SpanContainsFn.class);
    add(RegexFn.class);
    add(RegexTestFn.class);
    add(RegexMatchFn.class);
    add(RegexSpansFn.class);
    add(TokenizeFn.class);
    add(IsnullFn.class);
    add(DenullFn.class);
    add(DeemptyFn.class);
    add(StartsWithFn.class);
    add(SubstringFn.class);
    add(RecordFn.class);
    add(ArityFn.class);
    add(PairwiseFn.class);
    add(NullElementOnEmptyFn.class);
    add(NullOnEmptyFn.class);
    add(SerializeFn.class);
    add(JaqlGetFn.class);
    add(RemoveFieldsFn.class);
    add(FieldsFn.class);
    add(HasFieldFn.class);
    add(NamesFn.class);
    add(ValuesFn.class);
    add(RemapFn.class);
    add(RenameFieldsFn.class);
    add(AppendFn.class);
    add(ReplaceElementFn.class);
    add(RemoveElementFn.class);
    add(StReadExpr.class);
    add(StWriteExpr.class);
    add(InlinePragma.class);
    add(ConstPragma.class);
    add(AsArrayFn.class);
    add(EnumerateExpr.class);
    // add(CombinerExpr.class);
    add(FirstNonNullFn.class);
    add(EmptyOnNullFn.class);
    add(PairFn.class);
    add(ExpFn.class);
    add(LnFn.class);
    add(RandomLongFn.class);
    add(DistinctFn.class);
    // data access expressions
    add(ReadExpr.class);
    add(WriteExpr.class);
    add(LocalWriteExpr.class);
    add(LocalReadExpr.class);
    add(HdfsWriteExpr.class);
    add(HdfsReadExpr.class);
    add(HadoopTempExpr.class);
    add(HBaseWriteExpr.class);
    add(HBaseFetchExpr.class);
    add(HBaseDeleteExpr.class);
    add(HBaseReadExpr.class);
    add(ArrayReadExpr.class);
    add(HttpGetExpr.class);
    // store registration expressions
    add(RegisterAdapterExpr.class);
    add(UnregisterAdapterExpr.class);
    add(WriteAdapterRegistryExpr.class);
    add(ReadAdapterRegistryExpr.class);
    // function registration expressions
    add(RegisterFunctionExpr.class);
    add(WriteFunctionRegistryExpr.class);
    add(ReadFunctionRegistryExpr.class);
    // rand expressions
    add(RegisterRNGExpr.class);
    add(SampleRNGExpr.class);
    add(ReadConfExpr.class);
    // lower level shell access
    add(HdfsShellExpr.class);
    add(HBaseShellExpr.class);
  }

  /**
   * @param cls
   * @param env
   * @param args
   * @return
   */
  private static Expr lookupExpr(Class<? extends Expr> cls, Env env,
      ArrayList<Expr> args)
  {
    String name = cls.getName();
    JaqlFn fn = cls.getAnnotation(JaqlFn.class);
    if (fn == null)
    {
      throw new RuntimeException(
          "Builtin function must be annotated as JaqlFn: " + name);
    }
    if (args.size() < fn.minArgs())
    {
      throw new RuntimeException("function " + name + " expects at least "
          + fn.minArgs() + " arguments");
    }
    if (args.size() > fn.maxArgs())
    {
      throw new RuntimeException("function " + name + " expects at most "
          + fn.maxArgs() + " arguments");
    }

    Expr[] a = (args.size() == 0) ? Expr.NO_EXPRS : args.toArray(new Expr[args
        .size()]);

    try
    {
      Constructor<? extends Expr> cons = cls.getConstructor(Expr[].class);
      Expr expr = cons.newInstance(new Object[]{a}); // TODO: memory

      // TODO: Good chance I will want to defer macro expansion until some rewrites happen.
      if (expr instanceof MacroExpr)
      {
        MacroExpr macro = (MacroExpr) expr;
        expr = macro.expand(env);
      }

      return expr;
    }
    catch (Exception ex)
    {
      throw (ex instanceof RuntimeException)
          ? (RuntimeException) ex
          : new java.lang.reflect.UndeclaredThrowableException(ex);
    }
  }

  /**
   * @param cls
   * @return
   */
  @SuppressWarnings("unchecked")
  private static Class<? extends Expr> asExprClass(Class<?> cls)
  {
    if (Expr.class.isAssignableFrom(cls))
    {
      return (Class<? extends Expr>) cls;
    }
    return null;
  }

  /**
   * @param env
   * @param name
   * @param args
   * @return
   */
  public static Expr lookup(Env env, String name, ArrayList<Expr> args)
  {
    Class<?> cls = lib.get(name);
    if (cls == null)
    {
      throw new RuntimeException("function not found: " + name);
    }
    Class<? extends Expr> exprCls = asExprClass(cls);
    if (exprCls != null)
    {
      return lookupExpr(exprCls, env, args);
    }
    else
    {
      return new JavaFnExpr(name, cls, args);
    }
  }

  /**
   * @param env
   * @param name
   * @param arg
   * @return
   */
  public static Expr lookup(Env env, String name, Expr arg)
  {
    ArrayList<Expr> args = new ArrayList<Expr>(1);
    args.add(arg);
    return lookup(env, name, args); // TODO: lookup that takes Expr[] to save multiple conversions
  }
}

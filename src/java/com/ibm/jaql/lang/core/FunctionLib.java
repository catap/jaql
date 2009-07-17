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

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.agg.AnyAgg;
import com.ibm.jaql.lang.expr.agg.ArgMaxAgg;
import com.ibm.jaql.lang.expr.agg.ArgMinAgg;
import com.ibm.jaql.lang.expr.agg.ArrayAgg;
import com.ibm.jaql.lang.expr.agg.AvgAgg;
import com.ibm.jaql.lang.expr.agg.CountAgg;
import com.ibm.jaql.lang.expr.agg.CovStatsAgg;
import com.ibm.jaql.lang.expr.agg.MaxAgg;
import com.ibm.jaql.lang.expr.agg.MinAgg;
import com.ibm.jaql.lang.expr.agg.PickNAgg;
import com.ibm.jaql.lang.expr.agg.SingletonAgg;
import com.ibm.jaql.lang.expr.agg.SumAgg;
import com.ibm.jaql.lang.expr.agg.VectorSumAgg;
import com.ibm.jaql.lang.expr.array.AppendFn;
import com.ibm.jaql.lang.expr.array.ArrayToRecordFn;
import com.ibm.jaql.lang.expr.array.AsArrayFn;
import com.ibm.jaql.lang.expr.array.ColumnwiseFn;
import com.ibm.jaql.lang.expr.array.DeemptyFn;
import com.ibm.jaql.lang.expr.array.DistinctFn;
import com.ibm.jaql.lang.expr.array.EnumerateExpr;
import com.ibm.jaql.lang.expr.array.ExistsFn;
import com.ibm.jaql.lang.expr.array.MergeFn;
import com.ibm.jaql.lang.expr.array.PairFn;
import com.ibm.jaql.lang.expr.array.PairwiseFn;
import com.ibm.jaql.lang.expr.array.RemoveElementFn;
import com.ibm.jaql.lang.expr.array.ReplaceElementFn;
import com.ibm.jaql.lang.expr.array.ReverseFn;
import com.ibm.jaql.lang.expr.array.RowwiseFn;
import com.ibm.jaql.lang.expr.array.ShiftFn;
import com.ibm.jaql.lang.expr.array.SliceFn;
import com.ibm.jaql.lang.expr.array.ToArrayFn;
import com.ibm.jaql.lang.expr.binary.Base64Fn;
import com.ibm.jaql.lang.expr.binary.HexFn;
import com.ibm.jaql.lang.expr.core.CombineExpr;
import com.ibm.jaql.lang.expr.core.CompareFn;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.GroupCombineFn;
import com.ibm.jaql.lang.expr.core.IndexExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.lang.expr.core.MacroExpr;
import com.ibm.jaql.lang.expr.core.MergeContainersFn;
import com.ibm.jaql.lang.expr.core.PerPartitionFn;
import com.ibm.jaql.lang.expr.core.PerfFn;
import com.ibm.jaql.lang.expr.core.RangeExpr;
import com.ibm.jaql.lang.expr.core.TeeExpr;
import com.ibm.jaql.lang.expr.date.DateFn;
import com.ibm.jaql.lang.expr.date.DateMillisFn;
import com.ibm.jaql.lang.expr.date.DatePartsFn;
import com.ibm.jaql.lang.expr.date.NowFn;
import com.ibm.jaql.lang.expr.db.JdbcExpr;
import com.ibm.jaql.lang.expr.hadoop.MRAggregate;
import com.ibm.jaql.lang.expr.hadoop.MapReduceFn;
import com.ibm.jaql.lang.expr.hadoop.ReadConfExpr;
import com.ibm.jaql.lang.expr.index.BuildJIndexFn;
import com.ibm.jaql.lang.expr.index.BuildLuceneFn;
import com.ibm.jaql.lang.expr.index.KeyLookupFn;
import com.ibm.jaql.lang.expr.index.ProbeJIndexFn;
import com.ibm.jaql.lang.expr.index.ProbeLuceneFn;
import com.ibm.jaql.lang.expr.internal.ExprTreeExpr;
import com.ibm.jaql.lang.expr.internal.HashExpr;
import com.ibm.jaql.lang.expr.internal.LongHashExpr;
import com.ibm.jaql.lang.expr.io.ArrayReadExpr;
import com.ibm.jaql.lang.expr.io.FileFn;
import com.ibm.jaql.lang.expr.io.HBaseDeleteExpr;
import com.ibm.jaql.lang.expr.io.HBaseFetchExpr;
import com.ibm.jaql.lang.expr.io.HBaseReadExpr;
import com.ibm.jaql.lang.expr.io.HBaseShellExpr;
import com.ibm.jaql.lang.expr.io.HBaseWriteExpr;
import com.ibm.jaql.lang.expr.io.HadoopTempExpr;
import com.ibm.jaql.lang.expr.io.HdfsFn;
import com.ibm.jaql.lang.expr.io.HdfsShellExpr;
import com.ibm.jaql.lang.expr.io.HttpFn;
import com.ibm.jaql.lang.expr.io.HttpGetExpr;
import com.ibm.jaql.lang.expr.io.LocalReadFn;
import com.ibm.jaql.lang.expr.io.LocalWriteFn;
import com.ibm.jaql.lang.expr.io.ReadAdapterRegistryExpr;
import com.ibm.jaql.lang.expr.io.ReadFn;
import com.ibm.jaql.lang.expr.io.RegisterAdapterExpr;
import com.ibm.jaql.lang.expr.io.UnregisterAdapterExpr;
import com.ibm.jaql.lang.expr.io.WriteAdapterRegistryExpr;
import com.ibm.jaql.lang.expr.io.WriteFn;
import com.ibm.jaql.lang.expr.net.JaqlGetFn;
import com.ibm.jaql.lang.expr.nil.DenullFn;
import com.ibm.jaql.lang.expr.nil.EmptyOnNullFn;
import com.ibm.jaql.lang.expr.nil.FirstNonNullFn;
import com.ibm.jaql.lang.expr.nil.NullElementOnEmptyFn;
import com.ibm.jaql.lang.expr.nil.NullOnEmptyFn;
import com.ibm.jaql.lang.expr.nil.OnEmptyFn;
import com.ibm.jaql.lang.expr.numeric.AbsFn;
import com.ibm.jaql.lang.expr.numeric.DivFn;
import com.ibm.jaql.lang.expr.numeric.DoubleFn;
import com.ibm.jaql.lang.expr.numeric.ExpFn;
import com.ibm.jaql.lang.expr.numeric.IntFn;
import com.ibm.jaql.lang.expr.numeric.LnFn;
import com.ibm.jaql.lang.expr.numeric.ModFn;
import com.ibm.jaql.lang.expr.numeric.NumberFn;
import com.ibm.jaql.lang.expr.numeric.PowFn;
import com.ibm.jaql.lang.expr.numeric.ToNumberFn;
import com.ibm.jaql.lang.expr.pragma.ConstPragma;
import com.ibm.jaql.lang.expr.pragma.InlinePragma;
import com.ibm.jaql.lang.expr.random.RandomDoubleFn;
import com.ibm.jaql.lang.expr.random.RandomLongFn;
import com.ibm.jaql.lang.expr.random.RegisterRNGExpr;
import com.ibm.jaql.lang.expr.random.SampleRNGExpr;
import com.ibm.jaql.lang.expr.record.ArityFn;
import com.ibm.jaql.lang.expr.record.FieldsFn;
import com.ibm.jaql.lang.expr.record.NamesFn;
import com.ibm.jaql.lang.expr.record.RecordFn;
import com.ibm.jaql.lang.expr.record.RemapFn;
import com.ibm.jaql.lang.expr.record.RemoveFieldsFn;
import com.ibm.jaql.lang.expr.record.RenameFieldsFn;
import com.ibm.jaql.lang.expr.record.ReplaceFieldsFn;
import com.ibm.jaql.lang.expr.record.ValuesFn;
import com.ibm.jaql.lang.expr.regex.RegexExtractFn;
import com.ibm.jaql.lang.expr.regex.RegexFn;
import com.ibm.jaql.lang.expr.regex.RegexMatchFn;
import com.ibm.jaql.lang.expr.regex.RegexSpansFn;
import com.ibm.jaql.lang.expr.regex.RegexTestFn;
import com.ibm.jaql.lang.expr.schema.CheckFn;
import com.ibm.jaql.lang.expr.schema.SchemaOfExpr;
import com.ibm.jaql.lang.expr.schema.TypeOfExpr;
import com.ibm.jaql.lang.expr.span.SpanContainsFn;
import com.ibm.jaql.lang.expr.span.SpanFn;
import com.ibm.jaql.lang.expr.span.SpanOverlapsFn;
import com.ibm.jaql.lang.expr.span.TokenizeFn;
import com.ibm.jaql.lang.expr.string.EndsWithFn;
import com.ibm.jaql.lang.expr.string.SerializeFn;
import com.ibm.jaql.lang.expr.string.StartsWithFn;
import com.ibm.jaql.lang.expr.string.StrJoinFn;
import com.ibm.jaql.lang.expr.string.StrSplitNFn;
import com.ibm.jaql.lang.expr.string.StrcatFn;
import com.ibm.jaql.lang.expr.string.SubstringFn;
import com.ibm.jaql.lang.expr.system.ExecFn;
import com.ibm.jaql.lang.expr.system.RFn;
import com.ibm.jaql.lang.expr.udf.JavaFnExpr;
import com.ibm.jaql.lang.expr.xml.XmlToJsonFn;
import com.ibm.jaql.lang.registry.ReadFunctionRegistryExpr;
import com.ibm.jaql.lang.registry.RegisterFunctionExpr;
import com.ibm.jaql.lang.registry.WriteFunctionRegistryExpr;

/** Global libary of JAQL functions. Maps function names to implementing classes. 
 * 
 */
public class FunctionLib
{
  // private static HashMap<String, FnDecl> lib = new HashMap<String, FnDecl>();
  private static HashMap<String, Class<?>> lib = new HashMap<String, Class<?>>();

  /** Adds a built-in function to the library. The argument is required to carry the 
   * {@link JaqlFn} annotation. The name of the function is extracted from this annotation. 
   * @param cls
   */
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

  /** Adds a user-defined function to the library using the specified function name.
   * 
   * @param fnName
   * @param cls
   */
  public static void add(String fnName, Class<?> cls)
  {
    // assert cls.getAnnotation(JaqlFn.class) == null; // builtins should use single arg add() method
    lib.put(fnName, cls);
  }

  static
  {
    // TODO: add "import extension" that loads the functions in some jar (and loads types?)
    // schema
    add(TypeOfExpr.class);
    add(SchemaOfExpr.class);
    add(CheckFn.class);
    //    
    add(CompareFn.class);
    add(ExistsFn.class);
    //lib.put("loadXml", LoadXmlExpr.class);
    //lib.put("deepCompare", DeepCompareExpr.class);
    add(NowFn.class);
    add(DateFn.class);
    add(DateMillisFn.class);
    add(DatePartsFn.class);
    add(HexFn.class);
    add(Base64Fn.class);
    add(CountAgg.class);
    add(SumAgg.class);
    add(MinAgg.class);
    add(MaxAgg.class);
    add(AvgAgg.class);
    add(ArrayAgg.class);
    add(SingletonAgg.class);
    add(AnyAgg.class);
    add(PickNAgg.class);
    add(CombineExpr.class);
    add(ArgMaxAgg.class);
    add(ArgMinAgg.class);
    add(CovStatsAgg.class); // experimental
    add(VectorSumAgg.class); // experimental
    add(GroupCombineFn.class); // experimental
    add(TeeExpr.class);
    add(PerPartitionFn.class);
    add(PerfFn.class);
    add(ShiftFn.class);
    add(ModFn.class);
    add(DivFn.class);
    add(AbsFn.class);
    add(IntFn.class);
    add(NumberFn.class);
    add(DoubleFn.class);
    add(ToNumberFn.class);
    add(MapReduceFn.class);
    add(MRAggregate.class);
    //    add(MRCogroup.class);
    // add(DefaultExpr.class);
    add(JdbcExpr.class);
    add(SpanFn.class);
    add(SpanOverlapsFn.class);
    add(SpanContainsFn.class);
    add(RegexFn.class);
    add(RegexTestFn.class);
    add(RegexMatchFn.class);
    add(RegexSpansFn.class);
    add(RegexExtractFn.class);
    add(TokenizeFn.class);
    add(XmlToJsonFn.class);
    //add(IsnullExpr.class);
    add(DenullFn.class);
    add(DeemptyFn.class);
    add(StartsWithFn.class);
    add(EndsWithFn.class);
    add(SubstringFn.class);
    add(SerializeFn.class);
    add(StrcatFn.class);
    add(StrSplitNFn.class);
    add(StrJoinFn.class);
    add(RecordFn.class);
    add(ArityFn.class);
    add(PairwiseFn.class);
    add(NullElementOnEmptyFn.class);
    add(NullOnEmptyFn.class);
    add(JaqlGetFn.class);
    add(RemoveFieldsFn.class);
    add(FieldsFn.class);
    // add(IsdefinedExpr.class);
    add(NamesFn.class);
    add(ValuesFn.class);
    add(ArrayToRecordFn.class);
    add(RemapFn.class);
    add(ReplaceFieldsFn.class);
    add(RenameFieldsFn.class);
    add(AppendFn.class);
    add(ColumnwiseFn.class);
    add(RowwiseFn.class);
    add(SliceFn.class);
    add(IndexExpr.class);
    add(ExecFn.class);
    add(RFn.class);
    add(ReplaceElementFn.class);
    add(RemoveElementFn.class);
    add(InlinePragma.class);
    add(ConstPragma.class);
    add(AsArrayFn.class);
    add(ToArrayFn.class);
    add(EnumerateExpr.class);
    add(RangeExpr.class);
    // add(CombinerExpr.class);
    add(MergeFn.class);
    add(MergeContainersFn.class);
    add(ReverseFn.class);
    add(OnEmptyFn.class);
    add(FirstNonNullFn.class);
    add(EmptyOnNullFn.class);
    add(PairFn.class);
    add(ExpFn.class);
    add(LnFn.class);
    add(PowFn.class);
    add(RandomLongFn.class);
    add(RandomDoubleFn.class);
    add(DistinctFn.class);
    // data access expressions
    add(ReadFn.class);
    add(WriteFn.class);
    add(LocalWriteFn.class);
    add(LocalReadFn.class);
    add(HdfsFn.class);
    add(FileFn.class);
    add(HttpFn.class);
 // TODO: delete: add(HdfsWriteExpr.class);
 // TODO: delete: add(HdfsReadExpr.class);
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
    add(KeyLookupFn.class); // experimental
    add(BuildLuceneFn.class); // TODO: TEMPORARY
    add(ProbeLuceneFn.class); // TODO: TEMPORARY
    add(BuildJIndexFn.class);
    add(ProbeJIndexFn.class);
    // internal
    add(ExprTreeExpr.class);
    add(HashExpr.class);
    add(LongHashExpr.class);
  }

  /** Creates an instance of the function represented by the given class, passing the 
   * specified arguments.
   *  
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
      
      // FIXME: This a a MAJOR HACK to get d'...' and date('...') or x'...' and hex('...') to behave the same.
      // FIXME: We need to unify JSON constructors and our functions.
      if( ( expr instanceof DateFn || 
            expr instanceof HexFn ||
            expr instanceof Base64Fn ) &&
          expr.isCompileTimeComputable().always() )
      {
        JsonValue val = expr.eval(null); // more HACKS: context not required for these functions
        expr = new ConstExpr(val);
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

  /** Converts the specified class, if possible, otherwise returns null. 
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

  /** Performs a lookup for the function represented by the given name and arguments in 
   * the global library, and, if found, creates an instance passing the specified arguments.
   * 
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
      throw new RuntimeException("function not found: "+name);
      // return new FunctionCallExpr(new ConstExpr(new JString(name)), args);  // TODO: make python function item or at least verify fn exists.
    }

    Class<? extends Expr> exprCls = asExprClass(cls);
    if (exprCls != null)
    {
      return lookupExpr(exprCls, env, args); // use built-in function
    }
    else
    {
      return new JavaFnExpr(name, cls, args); // use reflection to find method
    }
  }

  /** Performs a lookup for the function represented by the given name and argument in 
   * the global library, and, if found, creates an instance passing the specified argument.
   * 
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

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

import java.util.HashMap;
import java.util.Map;

import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonString;
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
import com.ibm.jaql.lang.expr.array.Lag1Fn;
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
import com.ibm.jaql.lang.expr.function.BuiltInExpr;
import com.ibm.jaql.lang.expr.function.BuiltInFunction;
import com.ibm.jaql.lang.expr.function.BuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.hadoop.BuildModelFn;
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
import com.ibm.jaql.lang.expr.io.DelFn;
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
import com.ibm.jaql.lang.expr.io.JaqlTempFn;
import com.ibm.jaql.lang.expr.io.LinesFn;
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
import com.ibm.jaql.lang.expr.number.AbsFn;
import com.ibm.jaql.lang.expr.number.DecfloatFn;
import com.ibm.jaql.lang.expr.number.DivFn;
import com.ibm.jaql.lang.expr.number.DoubleFn;
import com.ibm.jaql.lang.expr.number.ExpFn;
import com.ibm.jaql.lang.expr.number.LnFn;
import com.ibm.jaql.lang.expr.number.LongFn;
import com.ibm.jaql.lang.expr.number.ModFn;
import com.ibm.jaql.lang.expr.number.NumberFn;
import com.ibm.jaql.lang.expr.number.PowFn;
import com.ibm.jaql.lang.expr.number.ToNumberFn;
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
import com.ibm.jaql.lang.expr.schema.AssertFn;
import com.ibm.jaql.lang.expr.schema.CheckFn;
import com.ibm.jaql.lang.expr.schema.SchemaOfExpr;
import com.ibm.jaql.lang.expr.schema.TypeOfExpr;
import com.ibm.jaql.lang.expr.span.SpanContainsFn;
import com.ibm.jaql.lang.expr.span.SpanFn;
import com.ibm.jaql.lang.expr.span.SpanOverlapsFn;
import com.ibm.jaql.lang.expr.span.TokenizeFn;
import com.ibm.jaql.lang.expr.string.ConvertFn;
import com.ibm.jaql.lang.expr.string.EndsWithFn;
import com.ibm.jaql.lang.expr.string.JsonFn;
import com.ibm.jaql.lang.expr.string.SerializeFn;
import com.ibm.jaql.lang.expr.string.StartsWithFn;
import com.ibm.jaql.lang.expr.string.StrJoinFn;
import com.ibm.jaql.lang.expr.string.StrSplitNFn;
import com.ibm.jaql.lang.expr.string.StrcatFn;
import com.ibm.jaql.lang.expr.string.SubstringFn;
import com.ibm.jaql.lang.expr.system.BatchFn;
import com.ibm.jaql.lang.expr.system.ExecFn;
import com.ibm.jaql.lang.expr.system.RFn;
import com.ibm.jaql.lang.expr.xml.XmlToJsonFn;
import com.ibm.jaql.lang.registry.ReadFunctionRegistryExpr;
import com.ibm.jaql.lang.registry.RegisterFunctionExpr;
import com.ibm.jaql.lang.registry.WriteFunctionRegistryExpr;

/** Global libary of JAQL functions. Maps function names to implementing classes. 
 * 
 */
public class FunctionLib
{
  /** descriptor class name to built in function */
  public static final Map<String, BuiltInFunctionDescriptor> descriptorMap 
      = new HashMap<String, BuiltInFunctionDescriptor>();
  
  /** implementing class name to built in function */
  public static final Map<Class<? extends Expr>, BuiltInFunctionDescriptor> implementationMap 
      = new HashMap<Class<? extends Expr>, BuiltInFunctionDescriptor>();
  
  /** Adds a built-in function to the library. The argument is required to carry the 
   * {@link JaqlFn} annotation. The name of the function is extracted from this annotation. 
   * @param cls
   */
  static void register(Env env, BuiltInFunctionDescriptor descriptor)
  {
    // check args
    String descriptorClassName = descriptor.getClass().getName();
    if (descriptorMap.containsKey(descriptorClassName))
    {
      // repeated registration is ok
      return;
    }
    if (implementationMap.containsKey(descriptor.getImplementingClass()))
    {
      throw new IllegalArgumentException("implementing class " 
          + descriptor.getImplementingClass().getName() 
          + " registered using multiple descriptors: "
          + descriptor.getClass().getName()
          + ", " + implementationMap.get(descriptor.getImplementingClass()).getClass().getName());
    }
    
    // register
    Var v = env.scopeGlobal(descriptor.getName(), SchemaFactory.functionSchema());
    descriptorMap.put(descriptorClassName, descriptor);
    implementationMap.put(descriptor.getImplementingClass(), descriptor);
    v.expr = new BuiltInExpr(new ConstExpr(new JsonString(descriptorClassName)));
    v.isDefined = true;
  }
  
  public static void registerAll(Env env)
  {
    // TODO: add "import extension" that loads the functions in some jar (and loads types?)
    // schema
    register(env, new TypeOfExpr.Descriptor());
    register(env, new SchemaOfExpr.Descriptor());
    register(env, new CheckFn.Descriptor());
    register(env, new AssertFn.Descriptor());
    //    
    register(env, new CompareFn.Descriptor());
    register(env, new ExistsFn.Descriptor());
    register(env, new Lag1Fn.Descriptor());
    //lib.put("loadXml", LoadXmlExpr.Descriptor());
    //lib.put("deepCompare", DeepCompareExpr.Descriptor());
    register(env, new NowFn.Descriptor());
    register(env, new DateFn.Descriptor());
    register(env, new DateMillisFn.Descriptor());
    register(env, new DatePartsFn.Descriptor());
    register(env, new HexFn.Descriptor());
    register(env, new Base64Fn.Descriptor());
    register(env, new CountAgg.Descriptor());
    register(env, new SumAgg.Descriptor());
    register(env, new MinAgg.Descriptor());
    register(env, new MaxAgg.Descriptor());
    register(env, new AvgAgg.Descriptor());
    register(env, new ArrayAgg.Descriptor());
    register(env, new SingletonAgg.Descriptor());
    register(env, new AnyAgg.Descriptor());
    register(env, new PickNAgg.Descriptor());
    register(env, new CombineExpr.Descriptor());
    register(env, new ArgMaxAgg.Descriptor());
    register(env, new ArgMinAgg.Descriptor());
    register(env, new CovStatsAgg.Descriptor()); // experimental
    register(env, new VectorSumAgg.Descriptor()); // experimental
    register(env, new GroupCombineFn.Descriptor()); // experimental
    register(env, new TeeExpr.Descriptor());
    register(env, new PerPartitionFn.Descriptor());
    register(env, new PerfFn.Descriptor());
    register(env, new ShiftFn.Descriptor());
    register(env, new ModFn.Descriptor());
    register(env, new DivFn.Descriptor());
    register(env, new AbsFn.Descriptor());
    register(env, new LongFn.Descriptor());
    register(env, new NumberFn.Descriptor());
    register(env, new DoubleFn.Descriptor());
    register(env, new DecfloatFn.Descriptor());
    register(env, new ToNumberFn.Descriptor());
    register(env, new MapReduceFn.Descriptor());
    register(env, new MRAggregate.Descriptor());
    //    register(env, new MRCogroup.Descriptor());
    // register(env, new DefaultExpr.Descriptor());
    register(env, new JdbcExpr.Descriptor());
    register(env, new SpanFn.Descriptor());
    register(env, new SpanOverlapsFn.Descriptor());
    register(env, new SpanContainsFn.Descriptor());
    register(env, new RegexFn.Descriptor());
    register(env, new RegexTestFn.Descriptor());
    register(env, new RegexMatchFn.Descriptor());
    register(env, new RegexSpansFn.Descriptor());
    register(env, new RegexExtractFn.Descriptor());
    register(env, new TokenizeFn.Descriptor());
    register(env, new XmlToJsonFn.Descriptor());
    //register(env, new IsnullExpr.Descriptor());
    register(env, new DenullFn.Descriptor());
    register(env, new DeemptyFn.Descriptor());
    register(env, new StartsWithFn.Descriptor());
    register(env, new EndsWithFn.Descriptor());
    register(env, new SubstringFn.Descriptor());
    register(env, new BatchFn.Descriptor());
    register(env, new SerializeFn.Descriptor());
    register(env, new StrcatFn.Descriptor());
    register(env, new StrSplitNFn.Descriptor());
    register(env, new StrJoinFn.Descriptor());
    register(env, new ConvertFn.Descriptor());
    register(env, new JsonFn.Descriptor());
    register(env, new RecordFn.Descriptor());
    register(env, new ArityFn.Descriptor());
    register(env, new PairwiseFn.Descriptor());
    register(env, new NullElementOnEmptyFn.Descriptor());
    register(env, new NullOnEmptyFn.Descriptor());
    register(env, new JaqlGetFn.Descriptor());
    register(env, new RemoveFieldsFn.Descriptor());
    register(env, new FieldsFn.Descriptor());
    // register(env, new IsdefinedExpr.Descriptor());
    register(env, new NamesFn.Descriptor());
    register(env, new ValuesFn.Descriptor());
    register(env, new ArrayToRecordFn.Descriptor());
    register(env, new RemapFn.Descriptor());
    register(env, new ReplaceFieldsFn.Descriptor());
    register(env, new RenameFieldsFn.Descriptor());
    register(env, new AppendFn.Descriptor());
    register(env, new ColumnwiseFn.Descriptor());
    register(env, new RowwiseFn.Descriptor());
    register(env, new SliceFn.Descriptor());
    register(env, new IndexExpr.Descriptor());
    register(env, new ExecFn.Descriptor());
    register(env, new RFn.Descriptor());
    register(env, new ReplaceElementFn.Descriptor());
    register(env, new RemoveElementFn.Descriptor());
    register(env, new InlinePragma.Descriptor());
    register(env, new ConstPragma.Descriptor());
    register(env, new AsArrayFn.Descriptor());
    register(env, new ToArrayFn.Descriptor());
    register(env, new EnumerateExpr.Descriptor());
    register(env, new RangeExpr.Descriptor());
    // register(env, new CombinerExpr.Descriptor());
    register(env, new MergeFn.Descriptor());
    register(env, new MergeContainersFn.Descriptor());
    register(env, new ReverseFn.Descriptor());
    register(env, new OnEmptyFn.Descriptor());
    register(env, new FirstNonNullFn.Descriptor());
    register(env, new EmptyOnNullFn.Descriptor());
    register(env, new PairFn.Descriptor());
    register(env, new ExpFn.Descriptor());
    register(env, new LnFn.Descriptor());
    register(env, new PowFn.Descriptor());
    register(env, new RandomLongFn.Descriptor());
    register(env, new RandomDoubleFn.Descriptor());
    register(env, new DistinctFn.Descriptor());
    register(env, new ReadFn.Descriptor());
    register(env, new WriteFn.Descriptor());
    register(env, new LocalWriteFn.Descriptor());
    register(env, new LocalReadFn.Descriptor());
    register(env, new HdfsFn.Descriptor());
    register(env, new DelFn.Descriptor());
    register(env, new LinesFn.Descriptor());
    register(env, new FileFn.Descriptor());
    register(env, new HttpFn.Descriptor());
    register(env, new JaqlTempFn.Descriptor());
  // TODO: delete: register(env, new HdfsWriteExpr.Descriptor());
  // TODO: delete: register(env, new HdfsReadExpr.Descriptor());
    register(env, new HadoopTempExpr.Descriptor());
    register(env, new HBaseWriteExpr.Descriptor());
    register(env, new HBaseFetchExpr.Descriptor());
    register(env, new HBaseDeleteExpr.Descriptor());
    register(env, new HBaseReadExpr.Descriptor());
    register(env, new ArrayReadExpr.Descriptor());
    register(env, new HttpGetExpr.Descriptor());
    // store registration expressions
    register(env, new RegisterAdapterExpr.Descriptor());
    register(env, new UnregisterAdapterExpr.Descriptor());
    register(env, new WriteAdapterRegistryExpr.Descriptor());
    register(env, new ReadAdapterRegistryExpr.Descriptor());
    // function registration expressions
    register(env, new RegisterFunctionExpr.Descriptor());
    register(env, new WriteFunctionRegistryExpr.Descriptor());
    register(env, new ReadFunctionRegistryExpr.Descriptor());
    // rand expressions
    register(env, new RegisterRNGExpr.Descriptor());
    register(env, new SampleRNGExpr.Descriptor());
    register(env, new ReadConfExpr.Descriptor());
    // lower level shell access
    register(env, new HdfsShellExpr.Descriptor());
    register(env, new HBaseShellExpr.Descriptor());
    register(env, new KeyLookupFn.Descriptor()); // TODO: experimental
    register(env, new BuildLuceneFn.Descriptor()); // TODO: experimental
    register(env, new ProbeLuceneFn.Descriptor()); // TODO: experimental
    register(env, new BuildJIndexFn.Descriptor());
    register(env, new ProbeJIndexFn.Descriptor());
    register(env, new BuildModelFn.Descriptor()); // TODO: experimental
    // internal
    register(env, new ExprTreeExpr.Descriptor());
    register(env, new HashExpr.Descriptor());
    register(env, new LongHashExpr.Descriptor());
  }
  
  public static BuiltInFunctionDescriptor getBuiltInFunctionDescriptor(Class<? extends Expr> implementingClass)
  {
    BuiltInFunctionDescriptor d = implementationMap.get(implementingClass);
    return d;
  }
  
  public static BuiltInFunction getBuiltInFunction(String descriptorClassName)
  {
    BuiltInFunctionDescriptor d = descriptorMap.get(descriptorClassName);
    return new BuiltInFunction(d);
  }

//  /** Adds a user-defined function to the library using the specified function name.
//   * 
//   * @param fnName
//   * @param cls
//   */
//  @SuppressWarnings("unchecked")
//  public static void add(String fnName, Class<?> cls)
//  {
//    if (Expr.class.isAssignableFrom(cls))
//    {
//      Class<? extends Expr> c = (Class<? extends Expr>)cls;
//      BuiltInFunction f = new BuiltInFunction(c);
//      if (!f.getName().equals(fnName))
//      {
//        throw new IllegalArgumentException("function names do not match: " + fnName + " " + f.getName());
//      }
//      builtInFunctions.put(f.getName(), f);
//    }
//    else
//    {
//      userFunctions.put(fnName, cls);
//    }
//  }
//
//  static
//  {
//  }
//
//  /** Creates an instance of the function represented by the given class, passing the 
//   * specified arguments.
//   *  
//   * @param c
//   * @param env
//   * @param args
//   * @return
//   */
//  private static Expr lookupExpr(BuiltInFunction f, Env env, List<Expr> positionalArgs,
//      Map<JsonString, Expr> namedArgs)
//  {
//    // get description of function
//    BuiltInFunctionDescriptor d = f.getDescriptor();
//    Parameters p = d.getParameters();
//    Expr[] exprs = argsToExprs(p, positionalArgs, namedArgs);
//    
//    // construct the desired expression
//    try
//    {
//      Constructor<? extends Expr> constructor = 
//        f.getImplementingClass().getConstructor(Expr[].class);
//      Expr expr = constructor.newInstance(new Object[]{ exprs }); // TODO: memory
//
//      // TODO: Good chance I will want to defer macro expansion until some rewrites happen.
//      if (expr instanceof MacroExpr)
//      {
//        MacroExpr macro = (MacroExpr) expr;
//        expr = macro.expand(env);
//      }
//      
//      // FIXME: This a a MAJOR HACK to get d'...' and date('...') or x'...' and hex('...') to behave the same.
//      // FIXME: We need to unify JSON constructors and our functions.
//      if( ( expr instanceof DateFn || 
//            expr instanceof HexFn ||
//            expr instanceof Base64Fn ) &&
//          expr.isCompileTimeComputable().always() )
//      {
//        JsonValue val = expr.eval(null); // more HACKS: context not required for these functions
//        expr = new ConstExpr(val);
//      }
//
//      return expr;
//    }
//    catch (Exception ex)
//    {
//      throw (ex instanceof RuntimeException)
//          ? (RuntimeException) ex
//          : new java.lang.reflect.UndeclaredThrowableException(ex);
//    }
//  }
//
//  /** Takes a list of positional and named args of a function and creates the expressions for
//   * each of its parameters. */
//  public static Expr[] argsToExprs(Parameters pars, List<Expr> positionalArgs,
//      Map<JsonString, Expr> namedArgs)
//  {
//    // check number of positional arguments
//    int n = positionalArgs.size();
//    int m = pars.noParameters();
//    if (n > m || (n == m && pars.hasRepeatingParameter()))
//    {
//      if (!namedArgs.isEmpty())
//      {
//        throw new IllegalArgumentException("named arguments disallowed when repeating parameter used");
//      }
//      if (!pars.hasRepeatingParameter())
//      {
//        throw new IllegalArgumentException("too many arguments provided");
//      }
//      m = n;
//    }
//    else
//    {
//      // repeating parameter is not used
//      if (pars.hasRepeatingParameter())
//      {
//        --m;
//      }
//    }
//    Expr[] exprs = new Expr[m];
//
//    
//    // copy positional arguments
//    int i=0;
//    for (; i<n; i++)
//    {
//      exprs[i] = positionalArgs.get(i);
//      assert exprs[i] != null;
//      // TODO: validate schema?
//    }
//    
//    // copy named required arguments
//    for (; i<pars.noRequiredParameters(); i++)
//    {
//      JsonString name = pars.nameOf(i);
//      if (!namedArgs.containsKey(name))
//      {
//        throw new IllegalArgumentException("missing value of parameter \"" + name + "\"");
//      }
//      exprs[i] = namedArgs.remove(name);
//    }
//    
//    // copy named optional arguments
//    for (; i<m; i++)      
//    {
//      JsonString name = pars.nameOf(i);
//      if (namedArgs.containsKey(name))
//      {
//        exprs[i] = namedArgs.remove(name);
//      }
//      else
//      {
//        exprs[i] = new ConstExpr(pars.defaultOf(i));
//      }
//      assert exprs[i] != null;
//    }
//    
//    // check if we still have arguments    
//    if (!namedArgs.isEmpty())
//    {
//      throw new IllegalArgumentException("invalid or repeated argument: " + namedArgs.keySet());
//    }
//    
//    return exprs;
//  }
//
//  /** Performs a lookup for the function represented by the given name and arguments in 
//   * the global library, and, if found, creates an instance passing the specified arguments.
//   * 
//   * @param env
//   * @param name
//   * @param args
//   * @return
//   */
//  public static Expr lookup(Env env, String name, List<Expr> positionalArgs, Map<JsonString, Expr> namedArgs)
//  {
//    BuiltInFunction f = builtInFunctions.get(name);
//    if (f != null)
//    {
//      return lookupExpr(f, env, positionalArgs, namedArgs);
//    }
//    Class<?> c = userFunctions.get(name);
//    if (c != null)
//    {
//      if (!namedArgs.isEmpty()) throw new IllegalArgumentException("Call by name not (yet) supported for java functions");
//      return new JavaFnExpr(name, c, positionalArgs); // use reflection to find method
//    }
//    throw new RuntimeException("function not found: " + name);
//  }
}

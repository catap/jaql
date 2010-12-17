/// TODO: fix or remove this file
///*
// * Copyright (C) IBM Corp. 2009.
// * 
// * Licensed under the Apache License, Version 2.0 (the "License"); you may not
// * use this file except in compliance with the License. You may obtain a copy of
// * the License at
// * 
// * http://www.apache.org/licenses/LICENSE-2.0
// * 
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// * License for the specific language governing permissions and limitations under
// * the License.
// */
//package com.ibm.jaql.lang.expr.core;
//
//import java.io.PrintStream;
//import java.util.ArrayList;
//import java.util.HashSet;
//
//import com.ibm.jaql.json.type.Item;
//import com.ibm.jaql.json.type.JArray;
//import com.ibm.jaql.json.util.Iter;
//import com.ibm.jaql.lang.core.Context;
//import com.ibm.jaql.lang.core.Env;
//import com.ibm.jaql.lang.core.Var;
//import com.ibm.jaql.lang.expr.array.MergeFn;
//import com.ibm.jaql.lang.expr.record.IsdefinedExpr;
//import com.ibm.jaql.lang.expr.top.AssignExpr;
//import com.ibm.jaql.lang.util.ItemHashtable;
//import com.ibm.jaql.util.Bool3;
//
//public class PartitionExpr extends IterExpr
//{
//
//  // input Binding, by binding, per partition subpipe
//  public PartitionExpr(Expr[] inputs)
//  {
//    super(inputs);
//  }
//
//  private static Expr[] makeArgs(Var inVar, Expr input, Var byVar, Expr byExpr, Expr subpipe)
//  {
//    if( byExpr == null )
//    {
//      byExpr = new DefaultExpr();
//    }
//    return new Expr[]{
//        new BindingExpr(BindingExpr.Type.IN, inVar, null, input),
//        new BindingExpr(BindingExpr.Type.EQ, byVar, null, byExpr),
//        subpipe
//    };
//  }
//
//  /**
//   * 
//   * @param v
//   * @param input
//   * @param by can be null
//   * @param subpipe
//   */
//  public PartitionExpr(Var inVar, Expr input, Var byVar, Expr byExpr, Expr subpipe)
//  {
//    // TODO: codify pipe/iter variables
//    super(makeArgs(inVar,input,byVar,byExpr,subpipe));
//  }
//  
//  public final BindingExpr inBinding()
//  {
//    return (BindingExpr)exprs[0];
//  }
//
//  /**
//   * @return
//   */
//  public final BindingExpr byBinding()
//  {
//    return (BindingExpr)exprs[1];
//  }
//
//  public final Expr into()
//  {
//    return exprs[2];
//  }
//
//  /*
//   * (non-Javadoc)
//   * 
//   * @see com.ibm.jaql.lang.expr.core.Expr#isNull()
//   */
//  @Override
//  public Bool3 isNull()
//  {
//    return Bool3.FALSE;
//  }
//
//  /*
//   * (non-Javadoc)
//   * 
//   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
//   *      java.util.HashSet)
//   */
//  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
//      throws Exception
//  {
//    // "partition" v=eachVar ("by" (bn=avar "=")? e=sexpr )?
//    final BindingExpr in = inBinding();
//    final BindingExpr by = byBinding();
//    in.inExpr().decompile(exprText, capturedVars,emitLocation);
//    exprText.print("\n -> partition each ");
//    exprText.print(in.var.name());
//    exprText.print(" by ");
//    if( by.var != Var.unused )
//    {
//      exprText.print(by.var.name());
//      exprText.print(" = ");
//    }
//    by.eqExpr().decompile(exprText, capturedVars,emitLocation);
//    exprText.print("\n( ");
//    into().decompile(exprText, capturedVars,emitLocation);
//    exprText.print(" )\n");
//    capturedVars.remove(in.var);
//    capturedVars.remove(by.var);
//  }
//
//  /*
//   * (non-Javadoc)
//   * 
//   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
//   */
//  @Override
//  public Iter iter(final Context context) throws Exception
//  {
//    // TODO: the ItemHashtable is a real quick and dirty prototype.  We need to spill to disk, etc...
//    final BindingExpr in = inBinding();
//    final BindingExpr by = byBinding();
//    Iter iter = in.inExpr().iter(context);
//    final Expr into = into();
//    
//    Expr byExpr = by.eqExpr();
//    if( byExpr instanceof DefaultExpr )
//    {
//      // No need to partition
//      context.setVar(in.var, iter);
//      context.setVar(by.var, Item.nil);
//      return into.iter(context);
//    }
//
//    ItemHashtable temp = new ItemHashtable(1);
//    Item item;
//    while ((item = iter.next()) != null)
//    {
//      context.setVar(in.var, item);
//      Item byItem = byExpr.eval(context);
//      temp.add(0, byItem, item);
//    }
//
//    final ItemHashtable.Iterator tempIter = temp.iter();
//
//    return new Iter() {
//      Iter collectIter = Iter.empty;
//
//      public Item next() throws Exception
//      {
//        while (true)
//        {
//          Item item = collectIter.next();
//          if (item != null)
//          {
//            return item;
//          }
//
//          if (!tempIter.next())
//          {
//            return null;
//          }
//
//          JArray part = (JArray)tempIter.values(0).get();
//          context.setVar(by.var, tempIter.key());
//          context.setVar(in.var, part.iter());
//
//          collectIter = into.iter(context);
//        }
//      }
//    };
//  }
//  
//  
//  /**
//   *      
//   * @param env
//   * @param inputs A list of Bindings
//   *      Each binding defines a named pipe, pi->$vi
//   *      Each binding optionally defines a second field on var2 and has a second child expression, $vi2=ei2<$vi>
//   * @return
//   *     merge (<p1> -> map each $v1 { $v1, v12: e12<$v1> }),
//   *           (...)
//   *           (<pn> -> map each $vn { $vn, vn2: en2<$vn> })
//   */
//  public static Expr makeTaggedMerge(Env env, ArrayList<BindingExpr> inputs)
//  {
//    int n = inputs.size();
//    Expr[] mergeIn = new Expr[n];
//    
//    for( int i = 0 ; i < n ; i++ )
//    {
//      BindingExpr b = inputs.get(i);
//      assert b.numChildren() == 1 || b.numChildren() == 2;
//      Expr[] fields = new Expr[ b.numChildren() ];
//      fields[0] = new NameValueBinding( b.var, true ); // vi: $vi
//      if( b.numChildren() == 2 )
//      {
//        fields[1] = new NameValueBinding( b.var2.nameAsField(), b.child(1), false ); // vi2: ei2<$vi>
//        b.var2 = null;
//        b.removeChild(1);
//      }
//      Expr rec = new RecordExpr( fields );
//      mergeIn[i] = new TransformExpr(new BindingExpr(BindingExpr.Type.IN, b.var, null, b.child(0)), rec);
//    }
//    
//    Expr merge = new MergeFn(mergeIn);
//    return merge;
//  }
//
//  /**
//   *  split each $s
//   *    if isdefined $s.v1 |- map each $m $m.v1 -> $v1 -|
//   *    if isdefined $s.vn |- map each $m $m.vn -> $vn -|
//   * 
//   * @param env
//   * @param inputs
//   * @return
//   */
//  public static Expr makeTaggedSplit(Env env, Expr splitIn, ArrayList<BindingExpr> names, Expr subpipe)
//  {
//    int n = names.size();
//    Expr[] splitArgs = new Expr[n+1];
//    BindingExpr splitBinding = new BindingExpr(BindingExpr.Type.IN, env.makeVar("$"), env.makeVar("$split"), splitIn); // TODO: need to improve vars; when is var reused vs req two?
//    splitArgs[0] = splitBinding;
//    
//    for( int i = 0 ; i < n ; i++ )
//    {
//      BindingExpr b = names.get(i);
//      String inName = b.var.nameAsField();
//      
//      // if isdefined $s.vi |- map each $m $m.vi -> $vi' -|
//      Expr test = new IsdefinedExpr(splitBinding.var, inName); // if isdefined $s.vi
//      Var mapVar = env.makeVar("$");
//      Expr proj = new FieldValueExpr(mapVar, inName); // $.vi
//      Expr map = new TransformExpr(mapVar, new VarExpr(splitBinding.var2), proj); // map each $ $.vi
//      Var splitOutVar = env.makeVar(b.var.name());
//      AssignExpr set = new AssignExpr(splitOutVar, map); // -> $vi
//      subpipe.replaceVar(b.var, splitOutVar);
//      splitArgs[i+1] = new IfExpr(test, set);
//    }
//    
//    Expr split = new SplitExpr(splitArgs);
//    return split;
//  }
//  
////  /**
////   * Expand 
////   *    copartition (ea -> $a) by $k=e1, (eb -> $b) by $k=e2 |- e3 -|
////   * into
////   *    merge (ea -> map each $a { a: $a, k: e1<$a> }),
////   *          (eb -> map each $b { b: $b, k: e2<$b> }) 
////   *    -> partition each $p by $k = $p.k
////   *         |- do split each $s
////   *                  if isdefined $s.a |- map each $m $m.a -> $a1 -|
////   *                  if isdefined $s.b |- map each $m $m.b -> $b1 -|;
////   *               e3<$a=$a1,$b=$b1>;
////   *            end -|
////   * 
////   * Each Binding input:
////   *   var     input variable
////   *   var2    by key variable (same for all inputs)
////   *   expr[0] input expr
////   *   expr[1] by expr
////   *   
////   * @param inputs
////   * @param subpipe
////   * @return
////   */
////  public static Expr makeCopartition(Env env, ArrayList<BindingExpr> inputs, Expr subpipe)
////  {
////    // TODO: special-case one input
////    Var byVar = inputs.get(0).var2;
////    Var partVar = env.makeVar("$");
////    Expr merge = makeTaggedMerge(env, inputs);
////    Expr split = makeTaggedSplit(env, new VarExpr(partVar), inputs, subpipe);
////    Expr block = new DoExpr(split, subpipe);
////    Expr byExpr = new FieldValueExpr(partVar, byVar.nameAsField()); // $p.key
////    return new PartitionExpr(partVar, merge, byVar, byExpr, block);
////  }
//
//  /**
//   * Expand 
//   *    copartition (ea -> $a) by $k=e1, (eb -> $b) by $k=e2 |- e3 -|
//   * into
//   *    taggedMerge (ea->$a) with $k=e1, (eb -> $b) with $k=e2
//   *    -> partition each $p by $k = $p.k |- e3 -|
//   * 
//   * Each Binding input:
//   *   var     input variable
//   *   var2    by key variable (same for all inputs)
//   *   expr[0] input expr
//   *   expr[1] by expr
//   * 
//   * e3 works on a single stream which is the merge of $a and $b.
//   *   //TODO: should e3 get multiple streams or one merged stream???
//   *   
//   * @param inputs
//   * @param subpipe
//   * @return
//   */
//  public static Expr makeCopartition(Env env, ArrayList<BindingExpr> inputs, Var partVar, Expr subpipe)
//  {
//    // TODO: special-case one input
//    Var oldByVar = inputs.get(0).var2;
//    Var byVar = env.makeVar(oldByVar.name());
//    //Expr merge = makeTaggedMerge(env, inputs);
//    Expr merge = new TaggedMergeExpr(inputs); // TODO: use this or expanded form?
//    Expr byExpr = new FieldValueExpr(partVar, byVar.nameAsField()); // $p.key
//    subpipe.replaceVar(oldByVar, byVar);
//    return new PartitionExpr(partVar, merge, byVar, byExpr, subpipe);
//  }
//}

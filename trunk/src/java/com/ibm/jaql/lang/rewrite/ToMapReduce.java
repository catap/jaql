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
package com.ibm.jaql.lang.rewrite;

import java.util.ArrayList;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.array.DeemptyFn;
import com.ibm.jaql.lang.expr.core.AggregateExpr;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.DefineFunctionExpr;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.FilterExpr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.core.GroupByExpr;
import com.ibm.jaql.lang.expr.core.NameValueBinding;
import com.ibm.jaql.lang.expr.core.RecordExpr;
import com.ibm.jaql.lang.expr.core.TransformExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.hadoop.MRAggregate;
import com.ibm.jaql.lang.expr.hadoop.MapReduceFn;
import com.ibm.jaql.lang.expr.io.HadoopTempExpr;
import com.ibm.jaql.lang.expr.io.ReadFn;
import com.ibm.jaql.lang.expr.io.WriteFn;
import com.ibm.jaql.lang.expr.nil.DenullFn;
import com.ibm.jaql.lang.rewrite.Segment.Type;

/**
 * 
 */
public class ToMapReduce extends Rewrite
{
  boolean modified;

  /**
   * @param phase
   */
  public ToMapReduce(RewritePhase phase)
  {
    super(phase, Expr.class);
  }

  /**
   * @param seg
   * @throws Exception
   */
  private void visit(Segment seg) throws Exception
  {
    //
    // "smap" are map segments without a write at the top.
    // "temp" is a temp injected if necessary (no write on top of input segment)
    // map -> MR
    // group <- smap) => MR
    // group => MR + temp
    // group <-- smap)  => MRCG + temps
    //        |- ... )
    //        |- ( *
    //        |- ( ...
    // combine <- smap => MR
    // combine => MR + temp
    // seq -> run local
    //
    // ?? add putInCluster(<filedesc>) -> <filedesc>: if <filedesc> not map reducible then copy the file to a temp in hdfs (or add to map/reduce)
    //

    //
    // Recurse down the tree
    //
    for (Segment s = seg.firstChild; s != null; s = s.nextSibling)
    {
      visit(s);
    }

    // TODO: should group-by be simplified to just group with a for above it to iterate per group?

    switch (seg.type)
    {
      case MAP : {
        forToMap(seg);
        break;
      }
      case GROUP : {
        groupToMapReduce(seg);
        //        Segment s = seg.firstChild; 
        //        if( s.nextSibling == null ) // simple group-by
        //        {
        //          buildMapReduce(s, null, seg);
        //        }
        //        else // cogroup
        //        {
        //           buildMRCogroup(seg);
        //          for( int i = 0 ; s != null ; s = s.nextSibling, i++ )
        //          {
        //            if( s.includeWithParent )
        //            {
        //            }
        //            else
        //            {
        //            }
        //          }
        //        }
        break;
      }
// TODO: make work for AggregateExpr, any Aggregate fn, not old combine syntax
//      case COMBINE : {
//        combineToMapReduce(seg);
//        break;
//      }
      case INLINE_MAP : {
        // handled by parent
        break;
      }
      case SEQUENTIAL :
      case MAPREDUCE :
      default : {
        // run locally
      }
    }
  }

  //  private StReadExpr injectTemp(Expr expr)
  //  {
  //    Expr parent = expr.parent;
  //    int i = expr.getChildSlot();
  //    StReadExpr reader = new StReadExpr(new StWriteExpr(new HadoopTempExpr(), expr));
  //    parent.setChild(i, reader);
  //    return reader;
  //  }
  //
  //  private StReadExpr forceMapInput(Expr expr)
  //  {
  //    StReadExpr reader;
  //    if( expr instanceof StReadExpr )
  //    {
  //      reader = (StReadExpr) expr;
  //    }
  //    else
  //    {
  //      reader = injectTemp(expr);
  //    }
  //    if( ! reader.isMapReducible() )
  //    {
  //      throw new RuntimeException("NYI //FIXME: add moveToCluster() ");
  //    }
  //    return reader;
  //  }
  //
  //  private StWriteExpr forceMapOutput(Expr expr)
  //  {
  //    StWriteExpr writer;
  //    if( expr instanceof StWriteExpr )
  //    {
  //      writer = (StWriteExpr) expr;
  //    }
  //    else
  //    {
  //      StReadExpr reader = injectTemp(expr);
  //      writer = (StWriteExpr) reader.exprs[0];
  //    }
  //    if( ! writer.isMapReducible() )
  //    {
  //      throw new RuntimeException("NYI //FIXME: add moveOffCluster() ");
  //    }
  //    return writer;
  //  }

  /**
   * @param combineSeg
   * @throws Exception
   */
//  private void combineToMapReduce(Segment combineSeg) throws Exception
//  {
 // TODO: make work for AggregateExpr, any Aggregate fn, not old combine syntax
//    CombineExpr combine = (CombineExpr) combineSeg.primaryExpr;
//
//    Expr topParent = combine.parent();
//    int topSlot = combine.getChildSlot();
//
//    Segment mapSeg = combineSeg.firstChild;
//    assert mapSeg.type == Segment.Type.INLINE_MAP;
//    ReadFn reader = (ReadFn) mapSeg.primaryExpr;
//    assert reader.isMapReducible();
//
//    // FIXME: rewrite function should not have any parameters
//    Expr input = reader.rewriteToMapReduce(new RecordExpr(Expr.NO_EXPRS)); // TODO: change name (not rewriting, but does steal inputs)
//    Expr output = new HadoopTempExpr();
//
//    // make the init expr:
//    //   fn($ci) {  for $fj in <input>([$ci]) collect [ [null, $fj] ] }
//    Var mapVar = engine.env.makeVar("$ci");
//    Expr expr = new ArrayExpr(new VarExpr(mapVar));
//    reader.replaceInParent(expr);
//    BindingExpr binding = combine.binding();
//    Expr combineInput = binding.inExpr(); // combine input
//    Var forVar = engine.env.makeVar("$fj");
//    Expr keyValPair = new ArrayExpr(new ConstExpr(Item.nil),
//        new VarExpr(forVar));
//    Expr forExpr = new ForExpr(forVar, combineInput, new ArrayExpr(keyValPair));
//    DefineFunctionExpr initFn = new DefineFunctionExpr(null, new Var[]{mapVar},
//        forExpr);
//
//    // make the combine fn:
//    //   fn($nil, $a, $b) { <usingExpr>($a,$b) }
//    Var keyVar = engine.env.makeVar("$nil");
//    DefineFunctionExpr combineFn = new DefineFunctionExpr(null, new Var[]{
//        keyVar, binding.var, binding.var2}, combine.usingExpr());
//
//    // make the final fn:
//    //   fn($nil, $val) { [$val] }
//    keyVar = engine.env.makeVar("$nil");
//    Var valVar = engine.env.makeVar("$val");
//    // TODO: could push upper exprs into this reduce (it is serial anyway)
//    DefineFunctionExpr finalFn = new DefineFunctionExpr(null, new Var[]{keyVar,
//        valVar}, new ArrayExpr(new VarExpr(valVar)));
//
//    RecordExpr args = new RecordExpr(new Expr[]{
//        new NameValueBinding("input", input),
//        new NameValueBinding("output", output),
//        new NameValueBinding("init", initFn),
//        new NameValueBinding("combine", combineFn),
//        new NameValueBinding("final", finalFn)});
//
//    // TODO: set num reducers to one!
//    MRAggregate mr = new MRAggregate(args);
//    expr = new ReadFn(mr);
//    expr = new IndexExpr(expr, new ConstExpr(JLong.ZERO_ITEM)); // TODO: add firstExpr?
//    topParent.setChild(topSlot, expr);
//
//    combineSeg.root = expr;
//    combineSeg.type = Segment.Type.SEQUENTIAL;
//    combineSeg.firstChild = new Segment(Segment.Type.MAPREDUCE,
//        combineSeg.firstChild);
//    combineSeg.firstChild.root = combineSeg.firstChild.primaryExpr = mr;
//
//    modified = true;
//  }

  /**
   * 
   */
  private static class PerCombineState
  {
    Expr init;
    Expr using;
  }

  /**
   * 
   */
  private static class PerInputState
  {
    Var                        mapIn;
    Var                        combineKey;
    Var                        combineIn1;
    Var                        combineIn2;
    ArrayList<PerCombineState> combineStates = new ArrayList<PerCombineState>();
    // Expr                       mapValueExpr;

    public PerInputState(Env env, int i)
    {
      this.mapIn = env.makeVar("$mapIn" + i);;
      this.combineKey = env.makeVar("$ckey" + i);
      this.combineIn1 = env.makeVar("$ca" + i);
      this.combineIn2 = env.makeVar("$cb" + i);
    }
  }

  /**
   * @param reduceSeg
   * @param group
   * @param inputStates
   * @param combineFns
   */
  private void buildCombiners(Segment reduceSeg, GroupByExpr group,
      PerInputState[] inputStates, Expr[] combineFns)
  {
    
    
    
 // TODO: make work for AggregateExpr, any Aggregate fn, not old combine syntax
//    int n = group.numInputs();
//
//    assert reduceSeg.type == Segment.Type.FINAL_GROUP;
//    for (Segment combineSeg = reduceSeg.firstChild; combineSeg != null; combineSeg = combineSeg.nextSibling)
//    {
//      // FINAL_GROUP with one or more COMBINE_GROUP children, each with exactly one MAP_GROUP.
//      assert combineSeg.type == Segment.Type.COMBINE_GROUP;
//      Segment mapSeg = combineSeg.firstChild;
//      assert mapSeg.nextSibling == null; // COMBINE_GROUP has exactly one chld
//      assert mapSeg.type == Segment.Type.MAP_GROUP;
//      assert mapSeg.firstChild == null; // MAP_GROUP has no children
//      CombineExpr combine = (CombineExpr) combineSeg.root;
//      VarExpr intoVarExpr = (VarExpr) mapSeg.primaryExpr;
//      Var intoVar = intoVarExpr.var();
//      int i = group.getIntoIndex(intoVar);
//      PerInputState inputState = inputStates[i];
//      int j = inputState.combineStates.size();
//      PerCombineState combineState = new PerCombineState();
//      inputState.combineStates.add(combineState);
//
//      // Copy the combine expression and its entire input tree for the map function.
//      // Replace the INTO var by [ $groupIn ].
//      BindingExpr inBinding = group.inBinding(i);
//      Expr expr = cloneExpr(combine);
//      replaceVarUses(intoVar, expr, new ArrayExpr(new VarExpr(inBinding.var)));
//      combineState.init = expr;
//
//      // Replace the combine expression in the final expression with $into[j]
//      expr = new VarExpr(inBinding.var2);
//      expr = new IndexExpr(expr, j);
//      combine.replaceInParent(expr);
//
//      // Make the combiner for this combine part:
//      //    if isNull($a[j]) then $b[j] 
//      //    else if isNull($b[j]) then $a[j]
//      //    else <using>($a[j],$b[j]) else null
//      expr = combine.usingExpr();
//      replaceVarUses(combine.binding().var, expr, new IndexExpr(new VarExpr(
//          inputState.combineIn1), j));
//      replaceVarUses(combine.binding().var2, expr, new IndexExpr(new VarExpr(
//          inputState.combineIn2), j));
//      expr = new IfExpr(new IsnullExpr(new IndexExpr(new VarExpr(
//          inputState.combineIn2), j)), new IndexExpr(new VarExpr(
//          inputState.combineIn1), j), expr);
//      expr = new IfExpr(new IsnullExpr(new IndexExpr(new VarExpr(
//          inputState.combineIn1), j)), new IndexExpr(new VarExpr(
//          inputState.combineIn2), j), expr);
//      combineState.using = expr;
//    }
//
//    for (int i = 0; i < n; i++)
//    {
//      PerInputState inputState = inputStates[i];
//      int m = inputState.combineStates.size();
//      Expr usingExpr;
//      // TODO: optimize out unnecessary arrays?  Need to fix IndexExpr references in the reducer too... 
//      //      if( m == 0 )
//      //      {
//      //        inputState.mapValueExpr = new ConstExpr(Item.nil);
//      //        usingExpr = new ConstExpr(Item.nil);
//      //      }
//      //      else if( m == 1 )
//      //      {
//      //        PerCombineState combineState = inputState.combineStates.get(0);
//      //        inputState.mapValueExpr = combineState.init;
//      //        usingExpr = combineState.using;
//      //      }
//      //      else
//      {
//        Expr[] inits = new Expr[m];
//        Expr[] usings = new Expr[m];
//        for (int j = 0; j < m; j++)
//        {
//          PerCombineState combineState = inputState.combineStates.get(j);
//          inits[j] = combineState.init;
//          usings[j] = combineState.using;
//        }
//        inputState.mapValueExpr = new ArrayExpr(inits);
//        usingExpr = new ArrayExpr(usings);
//      }
//      combineFns[i] = new DefineFunctionExpr(null, new Var[]{
//          inputState.combineKey, inputState.combineIn1, inputState.combineIn2},
//          usingExpr);
//    }
  }

  private void groupToMapReduce(Segment groupSeg)
  {
    Expr topParent = groupSeg.root.parent();
    int topSlot = groupSeg.root.getChildSlot();

    GroupByExpr group = (GroupByExpr) groupSeg.primaryExpr;
    if( group.numInputs() != 1 )
    {
      cogroupToMapReduce(groupSeg);
      return;
    }
    
    Segment reduceSeg = segmentReduce(group, group.collectExpr());
    if( reduceSeg.type == Type.SEQUENTIAL_GROUP )
    {
      cogroupToMapReduce(groupSeg);
      return;
    }
    
    assert reduceSeg.type == Type.FINAL_GROUP;
    
    AggregateExpr ae = null;
    Expr[] aggs;
    if( reduceSeg.firstChild == null )
    {
      aggs = new Expr[0];
    }
    else
    {
      assert reduceSeg.firstChild.type == Type.COMBINE_GROUP;
      ae = (AggregateExpr)reduceSeg.firstChild.root;
      aggs = new Expr[ae.numAggs()];
      for(int i = 0 ; i < aggs.length ; i++)
      {
        aggs[i] = ae.agg(i);
      }
    }

    Env env = engine.env;
    Var keyVar;
    Var valVar = group.inBinding().var;
    Expr expr = group.byBinding().byExpr(0);
    expr = new ArrayExpr(expr, new VarExpr(valVar));
    expr = new TransformExpr(group.inBinding(), expr);
    valVar = env.makeVar("$vals");
    Segment mapSeg = groupSeg.firstChild;
    assert mapSeg.type == Segment.Type.INLINE_MAP;
    assert mapSeg.nextSibling == null;
    // Replace the reader with $vals
    ReadFn reader = (ReadFn) mapSeg.primaryExpr;
    Expr input = reader.descriptor(); //reader.rewriteToMapReduce(new RecordExpr(Expr.NO_EXPRS));
    reader.replaceInParent(new VarExpr(valVar));
    Expr mapFn = new DefineFunctionExpr(null, new Var[]{valVar}, expr);    

    keyVar = env.makeVar(group.byVar().name());
    expr = new ArrayExpr(aggs);
    expr.replaceVar(group.byVar(), keyVar);
    if( ae != null )
    {
      valVar = ae.binding().var;
      expr.replaceVar(ae.binding().var, valVar);
    }
    else
    {
      valVar = env.makeVar("$unused");
    }
    Expr aggFn = new DefineFunctionExpr(null, new Var[]{keyVar,valVar}, expr);
    
    keyVar = env.makeVar(group.byVar().name());
    valVar = env.makeVar("$vals");
    expr = group.collectExpr();
    if( ae != null )
    {
      Expr inexpr = new ArrayExpr(new VarExpr(valVar));
      if( ae == expr )
      {
        expr = inexpr;
      }
      else
      {
        ae.replaceInParent(inexpr);
      }
    }
    expr.replaceVar(group.byVar(), keyVar);
    expr.replaceVar(group.inBinding().var, valVar);
    Expr finalFn = new DefineFunctionExpr(null, new Var[]{keyVar,valVar}, expr);

    Expr output;
    Expr lastExpr = groupSeg.root;
    boolean writing = lastExpr instanceof WriteFn;
    if (writing)
    {
      WriteFn writer = (WriteFn) groupSeg.root;
      assert writer.isMapReducible();
      // FIXME: rewrite function should not have any parameters
      output = writer.descriptor(); //writer.rewriteToMapReduce(new RecordExpr(Expr.NO_EXPRS)); // TODO: change name (not rewriting, but does steal inputs)
      lastExpr = writer.dataExpr();
    }
    else
    {
      output = new HadoopTempExpr();
    }

    Expr fnArgs[] = new Expr[] {
        new NameValueBinding("input", input),
        new NameValueBinding("output", output),
        new NameValueBinding("map", mapFn),
        new NameValueBinding("aggregate", aggFn),
        new NameValueBinding("final", finalFn),
    };
    RecordExpr args = new RecordExpr(fnArgs);
    Expr mr = new MRAggregate(args);
    
    if (writing)
    {
      expr = mr;
      groupSeg.type = Segment.Type.MAPREDUCE;
      groupSeg.root = expr;
    }
    else
    {
      expr = new ReadFn(mr);
      groupSeg.type = Segment.Type.MAP; // NOW: group(group(T)) bug INLINE_MAP?
      groupSeg.root = expr;
      groupSeg.firstChild = new Segment(Segment.Type.MAPREDUCE,
          groupSeg.firstChild);
      groupSeg.firstChild.root = groupSeg.firstChild.primaryExpr = mr;
    }

    topParent.setChild(topSlot, expr);
    modified = true;
  }
  
  /**
   * @param groupSeg
   */
  private void cogroupToMapReduce(Segment groupSeg)
  {
    Expr topParent = groupSeg.root.parent();
    int topSlot = groupSeg.root.getChildSlot();

    GroupByExpr group = (GroupByExpr) groupSeg.primaryExpr;
    int n = group.numInputs();
    BindingExpr byBinding = group.byBinding();

    PerInputState[] inputStates = new PerInputState[n];
    Expr[] inputs = new Expr[n];
    Var[] reduceParams = new Var[n + 1];

    reduceParams[0] = byBinding.var;

    // Setup per input
    Segment mapSeg = groupSeg.firstChild;
    for (int i = 0; i < n; i++)
    {
      assert mapSeg.type == Segment.Type.INLINE_MAP;
      PerInputState inputState = inputStates[i] = new PerInputState(engine.env, i);

      // Replace the reader with [ $mapIn ]
      ReadFn reader = (ReadFn) mapSeg.primaryExpr;
      assert reader.isMapReducible();
      inputs[i] = reader.descriptor(); //rewriteToMapReduce(new RecordExpr(Expr.NO_EXPRS)); // TODO: change name (not rewriting, but does steal inputs)
      //Expr expr = new ArrayExpr(new VarExpr(inputState.mapIn));
      Expr expr = new VarExpr(inputState.mapIn);
      reader.replaceInParent(expr);

      // Build the initial map expression
      Var asVar = group.getAsVar(i);
      //inputStates[i].mapValueExpr = new VarExpr(asVar);

      // Use the group as variables for the reduce/final function
      reduceParams[i + 1] = asVar;

      mapSeg = mapSeg.nextSibling;
    }

    String mapName = "map";
    String reduceName = "reduce";
    Expr[] combineFns = null;
    Segment reduceSeg = segmentReduce(group, group.collectExpr());
    boolean combining = (reduceSeg.type == Segment.Type.FINAL_GROUP);
    if (combining)
    {
      // We are running combiners!
      // (or there are no combiners, and no references to any of the INTO variables)
      mapName = "init";
      reduceName = "final";
      combineFns = new Expr[n];
      buildCombiners(reduceSeg, group, inputStates, combineFns);
    }

    // build the map/init functions
    Expr[] mapFns = new Expr[n];
    for (int i = 0; i < n; i++)
    {
      Var v = engine.env.makeVar("$i"+i);
      PerInputState inputState = inputStates[i];
      BindingExpr b = group.inBinding();
      Expr inExpr = b.child(i);
      Expr byExpr = byBinding.byExpr(i);
      byExpr.replaceVar(b.var, v);
      Expr keyValPair = new ArrayExpr(byExpr, new VarExpr(v));
      Expr forExpr = new ForExpr(v, inExpr, new ArrayExpr(keyValPair));
      mapFns[i] = new DefineFunctionExpr(null, new Var[]{inputState.mapIn}, forExpr);
    }

    // Make the output
    Expr output;
    Expr lastExpr = groupSeg.root;
    boolean writing = lastExpr instanceof WriteFn;
    if (writing)
    {
      WriteFn writer = (WriteFn) groupSeg.root;
      assert writer.isMapReducible();
      output = writer.descriptor(); //rewriteToMapReduce(new RecordExpr(Expr.NO_EXPRS)); // TODO: change name (not rewriting, but does steal inputs)
      lastExpr = writer.dataExpr();
    }
    else
    {
      output = new HadoopTempExpr();
    }

    // Build the final function, including any expressions above the group-by
    Expr reduce = group.collectExpr();
    if (group != lastExpr)
    {
      group.replaceInParent(reduce);
      reduce = lastExpr;
    }
    reduce = new DefineFunctionExpr(null, reduceParams, reduce);

    Expr[] fnArgs = new Expr[5];
    Expr input;
    Expr map;
    Expr combine = null;
    if (n == 1)
    {
      input = inputs[0];
      map = mapFns[0];
      if (combining)
      {
        combine = combineFns[0];
      }
    }
    else
    {
      input = new ArrayExpr(inputs);
      map = new ArrayExpr(mapFns);
      if (combining)
      {
        combine = new ArrayExpr(combineFns);
      }
    }
    if (combining)
    {
      fnArgs = new Expr[5];
      fnArgs[4] = new NameValueBinding("combine", combine);
    }
    else
    {
      fnArgs = new Expr[4];
    }
    fnArgs[0] = new NameValueBinding("input", input);
    fnArgs[1] = new NameValueBinding("output", output);
    fnArgs[2] = new NameValueBinding(mapName, map);
    fnArgs[3] = new NameValueBinding(reduceName, reduce);

    RecordExpr args = new RecordExpr(fnArgs);
    Expr mr;
    if (combining)
    {
      mr = new MRAggregate(args);
    }
    else
    {
      mr = new MapReduceFn(args);
    }

    Expr expr;
    if (writing)
    {
      expr = mr;
      groupSeg.type = Segment.Type.MAPREDUCE;
      groupSeg.root = expr;
    }
    else
    {
      expr = new ReadFn(mr);
      groupSeg.type = Segment.Type.MAP; // NOW: group(group(T)) bug INLINE_MAP?
      groupSeg.root = expr;
      groupSeg.firstChild = new Segment(Segment.Type.MAPREDUCE,
          groupSeg.firstChild);
      groupSeg.firstChild.root = groupSeg.firstChild.primaryExpr = mr;
    }

    topParent.setChild(topSlot, expr);
    modified = true;
  }

  /**
   * @param mapSeg
   */
  private void forToMap(Segment mapSeg)
  {
    assert mapSeg.type == Segment.Type.MAP;

    ReadFn reader = (ReadFn) mapSeg.primaryExpr;
    assert reader.isMapReducible();

    if (mapSeg.root == reader)
    {
      // read(T) is marked as a map segment, but it is silly to run it as map/reduce.
      // It probably shouldn't be marked as a map segment...
      return;
    }

    Expr topParent = mapSeg.root.parent();
    int topSlot = mapSeg.root.getChildSlot();

    Expr input = reader.descriptor(); //rewriteToMapReduce(new RecordExpr(Expr.NO_EXPRS)); // TODO: change name (not rewriting, but does steal inputs)
    Var mapIn = engine.env.makeVar("$mapIn");
    //Expr expr = new ArrayExpr(new VarExpr(mapIn));
    Expr expr = new VarExpr(mapIn);
    reader.replaceInParent(expr);

    // Make the output
    Expr output;
    Expr lastExpr = mapSeg.root;
    boolean writing = lastExpr instanceof WriteFn;
    if (writing)
    {
      WriteFn writer = (WriteFn) mapSeg.root;
      assert writer.isMapReducible();
      output = writer.descriptor(); //rewriteToMapReduce(new RecordExpr(Expr.NO_EXPRS)); // TODO: change name (not rewriting, but does steal inputs)
      lastExpr = writer.dataExpr();
    }
    else
    {
      output = new HadoopTempExpr();
    }

    // build key value pair:
    //   for $fv in <lastExpr> collect [[null, $fv]]
    Var forVar = engine.env.makeVar("$fv");
    expr = new ArrayExpr(new ConstExpr(Item.nil), new VarExpr(forVar));
    expr = new ForExpr(forVar, lastExpr, new ArrayExpr(expr));

    Expr mapFn = new DefineFunctionExpr(null, new Var[]{mapIn}, expr);

    expr = new MapReduceFn(new RecordExpr(new Expr[]{
        new NameValueBinding("input", input),
        new NameValueBinding("output", output),
        new NameValueBinding("map", mapFn)}));

    mapSeg.type = Segment.Type.MAPREDUCE;
    mapSeg.root = expr;

    if (!writing)
    {
      expr = new ReadFn(expr);
      //      mapSeg.firstChild = new Segment(Segment.Type.MAPREDUCE, mapSeg.firstChild);
      //      groupSeg.firstChild.root = groupSeg.firstChild.primaryExpr = mr;
    }

    topParent.setChild(topSlot, expr);
    modified = true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr) throws Exception
  {
    modified = false;
    Segment seg = segment(expr);
    visit(seg);
    return modified;
  }

  /**
   * @param expr
   * @return
   */
  protected Segment segment(Expr expr)
  {
    Segment seg;

    // FIXME: IfExpr, UnnestExpr

    if( expr instanceof TransformExpr ||
        expr instanceof FilterExpr ||
        expr instanceof ForExpr )
    {
      seg = segment(expr.child(0).child(0)); // binding input
      switch (seg.type)
      {
        case SEQUENTIAL :
          // the input is running sequentially,
          // -> the for loop will run sequentially, 
          // -> use the same segment
          // try to parallelize the return
          seg.addChild(segment(expr.child(1)));
          break;
        case MAP :
        case GROUP :
          if (mightContainMapReduce(expr.child(1))) // FIXME: this needs to look for other functions that cannot be relocated (eg, local read/write)
          {
            seg = new Segment(Segment.Type.SEQUENTIAL, seg);
          }
          break;
        default :
          seg = new Segment(Segment.Type.SEQUENTIAL, seg);
      }
    }
    else if (expr instanceof DoExpr)
    {
      // DoExprs are always run sequentially
      DoExpr doExpr = (DoExpr) expr;
      seg = new Segment(Segment.Type.SEQUENTIAL);
      int n = doExpr.numChildren();
      for (int i = 0; i < n; i++)
      {
        seg.addChild(segment(doExpr.child(i)));
      }
    }
    else if (expr instanceof GroupByExpr)
    {
      GroupByExpr group = (GroupByExpr) expr;
      seg = new Segment(Segment.Type.GROUP);
      seg.primaryExpr = group;
      int n = group.numInputs();
      for (int i = 0; i < n; i++)
      {
        Segment s = segment(group.inBinding().child(i));
        if (s.type == Segment.Type.GROUP || s.type == Segment.Type.COMBINE)
        {
          s = makeMapSegment(s);
        }
        if (s.type == Segment.Type.MAP)
        {
          s.type = Segment.Type.INLINE_MAP;
          seg.addChild(s);
        }
        else
        {
          seg.type = Segment.Type.SEQUENTIAL;
        }
      }
      if (seg.type == Segment.Type.SEQUENTIAL)
      {
        seg.mergeSequential();
      }
    }
 // TODO: make work for AggregateExpr, any Aggregate fn, not old combine syntax
//    else if (expr instanceof CombineExpr)
//    {
//      CombineExpr combine = (CombineExpr) expr;
//      Segment s = segment(combine.binding().inExpr());
//      if (s.type == Segment.Type.GROUP || s.type == Segment.Type.COMBINE)
//      {
//        s = makeMapSegment(s);
//      }
//      if (s.type == Segment.Type.MAP)
//      {
//        s.type = Segment.Type.INLINE_MAP;
//        seg = new Segment(Segment.Type.COMBINE, s);
//        seg.primaryExpr = combine;
//      }
//      else if (s.type == Segment.Type.SEQUENTIAL)
//      {
//        seg = s;
//      }
//      else
//      {
//        seg = new Segment(Segment.Type.SEQUENTIAL, s);
//      }
//    }
    else if (expr instanceof ReadFn)
    {
      ReadFn reader = (ReadFn) expr;
      Segment s = segment(reader.descriptor());
      if (reader.isMapReducible())
      {
        seg = new Segment(Segment.Type.MAP, s);
        seg.primaryExpr = expr;
      }
      else if (s.type == Segment.Type.SEQUENTIAL)
      {
        seg = s;
      }
      else
      {
        seg = Segment.sequential(s);
      }
    }
    else if (expr instanceof WriteFn)
    {
      WriteFn writer = (WriteFn) expr;
      Segment s1 = segment(writer.dataExpr()); // write input
      switch (s1.type)
      {
        case MAP :
        case GROUP :
        case COMBINE :
          if (writer.isMapReducible())
          {
            // merge into map reduce
            seg = s1;
            break;
          }
          // fall through
        default :
          // make sequential write but try to parallelize the descriptor
          Segment s0 = segment(writer.descriptor());
          if (s0.type == Segment.Type.SEQUENTIAL)
          {
            // merge into sequential segment(s)
            seg = s0;
            seg.addChild(s1);
          }
          else if (s1.type == Segment.Type.SEQUENTIAL)
          {
            seg = s1;
            seg.addChild(s0);
          }
          else
          {
            seg = new Segment(Segment.Type.SEQUENTIAL);
            seg.addChild(s0);
            seg.addChild(s1);
          }
      }
    }
    else if (expr instanceof MapReduceFn || expr instanceof MRAggregate)
    {
      seg = new Segment(Segment.Type.MAPREDUCE);
      seg.primaryExpr = expr;
      // FIXME: dig into input/output expressions
    }
    else if (expr instanceof DenullFn || expr instanceof DeemptyFn)
    {
      seg = segment(expr.child(0));
      if (!(seg.type == Segment.Type.SEQUENTIAL || seg.type == Segment.Type.MAP))
      {
        seg = new Segment(Segment.Type.SEQUENTIAL, seg);
      }
    }
    else if (expr instanceof DefineFunctionExpr)
    {
      // Run sequentially and don't segment the function body
      seg = new Segment(Segment.Type.SEQUENTIAL);
    }
    else
    {
      // run sequentially, but segment the child expressions.
      seg = new Segment(Segment.Type.SEQUENTIAL);
      for (Expr e : expr.children())
      {
        seg.addChild(segment(e));
      }
    }

    seg.root = expr;
    return seg;
  }

  private Segment makeMapSegment(Segment seg)
  {
    Expr root = seg.root;
    Expr parent = root.parent();
    int slot = root.getChildSlot();
    Expr tmp = new WriteFn(root, new HadoopTempExpr());
    Expr read = new ReadFn(tmp);
    parent.setChild(slot, read);
    if (seg.type == Segment.Type.GROUP || seg.type == Segment.Type.COMBINE)
    {
      seg.root = tmp;
    }
    seg = new Segment(Segment.Type.MAP, seg);
    seg.root = seg.primaryExpr = read;
    return seg;
  }

  /**
   * @param group
   * @param expr
   * @return
   */
  protected boolean segmentReduceIsLocal(GroupByExpr group, Expr expr)
  {
    if (expr instanceof VarExpr)
    {
      VarExpr ve = (VarExpr) expr;
      boolean notInto = group.getIntoIndex(ve.var()) < 0;
      return notInto;
    }
    else
    {
      // TODO: look for anything else here?
      for (Expr e : expr.children())
      {
        if (!segmentReduceIsLocal(group, e))
        {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * Returns one segment with no children that has type in: MAP_GROUP: input to
   * a combiner FINAL_GROUP: not good for a combiner, but ok with other
   * combiners SEQUENTIAL_GROUP: not good with any combiners
   * 
   * @param group
   * @param expr
   * @return
   */
  protected Segment segmentReduceCombine(GroupByExpr group, Expr expr)
  {
    Segment seg;
    if (expr instanceof VarExpr)
    {
      VarExpr ve = (VarExpr) expr;
      if (group.getIntoIndex(ve.var()) >= 0)
      {
        seg = new Segment(Segment.Type.MAP_GROUP);
        seg.primaryExpr = ve;
      }
      else if (false && segmentReduceIsLocal(group, expr))
      {
        seg = new Segment(Segment.Type.FINAL_GROUP);
      }
      else
      {
        seg = new Segment(Segment.Type.SEQUENTIAL_GROUP);
      }
    }
    else if (expr instanceof ForExpr ||
             expr instanceof TransformExpr ||
             expr instanceof FilterExpr )
    {
      if (segmentReduceIsLocal(group, expr.child(1)))
      {
        seg = segmentReduceCombine(group, expr.child(0).child(0));
      }
      else
      {
        seg = new Segment(Segment.Type.SEQUENTIAL_GROUP);
      }
    }
 // TODO: make work for AggregateExpr, any Aggregate fn, not old combine syntax
//    else if (expr instanceof CombineExpr) // chained combined expression - push into map phase
//    {
//      CombineExpr combine = (CombineExpr) expr;
//      seg = segmentReduceCombine(group, combine.binding().inExpr());
//    }
    else
    {
      if (false && segmentReduceIsLocal(group, expr))
      {
        seg = new Segment(Segment.Type.FINAL_GROUP);
      }
      else
      {
        seg = new Segment(Segment.Type.SEQUENTIAL_GROUP);
      }
    }
    seg.root = expr;
    return seg;
  }

  /**
   * Returns a Segment with type: 
   * 
   *    FINAL_GROUP: runs in mrAggregate final function
   *      may have one COMBINE_GROUP child segment:
   *         primary = an algebraic aggregate expression on the group
   *      my have no child segment:
   *         group values are not used, so make mrAggregate with no aggs.
   *         
   *    SEQUENTIAL_GROUP: no combiners, run in mapReduce reduce function
   *    
   * @param group
   * @param expr
   * @return
   */
  protected Segment segmentReduce(GroupByExpr group, Expr expr)
  {
    Segment seg = null;
    if( group.numInputs() == 1 )
    {
      Var v = group.getAsVar(0); 
      ArrayList<Expr> uses = new ArrayList<Expr>();
      group.collectExpr().getVarUses(v, uses);
      if( uses.size() == 0 )
      {
        seg = new Segment(Segment.Type.FINAL_GROUP);
        seg.root = seg.primaryExpr = null;
        seg.root = seg.primaryExpr = expr;
      }
      else if( uses.size() == 1 )
      {
        VarExpr ve = (VarExpr)uses.get(0);
        if( ve.parent() instanceof BindingExpr &&
            ve.parent().parent() instanceof AggregateExpr )
        {
          AggregateExpr ae = (AggregateExpr)ve.parent().parent();
          if( ae.isAlgebraic() )
          {
            seg = new Segment(Segment.Type.COMBINE_GROUP);
            seg.root = seg.primaryExpr = ae;
            seg = new Segment(Segment.Type.FINAL_GROUP, seg);
            seg.root = seg.primaryExpr = expr;
          }
        }
      }
    }

    if( seg == null )
    {
      seg = new Segment(Segment.Type.SEQUENTIAL_GROUP);
      seg.root = seg.primaryExpr = expr;
    }

    return seg;
  }

}

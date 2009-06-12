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
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.array.DeemptyFn;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.CombineExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.DefineFunctionExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.core.GroupByExpr;
import com.ibm.jaql.lang.expr.core.IfExpr;
import com.ibm.jaql.lang.expr.core.IndexExpr;
import com.ibm.jaql.lang.expr.core.LetExpr;
import com.ibm.jaql.lang.expr.core.NameValueBinding;
import com.ibm.jaql.lang.expr.core.RecordExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.hadoop.MRAggregate;
import com.ibm.jaql.lang.expr.hadoop.MapReduceFn;
import com.ibm.jaql.lang.expr.io.HadoopTempExpr;
import com.ibm.jaql.lang.expr.io.StReadExpr;
import com.ibm.jaql.lang.expr.io.StWriteExpr;
import com.ibm.jaql.lang.expr.nil.DenullFn;
import com.ibm.jaql.lang.expr.nil.IsnullFn;

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
      case COMBINE : {
        combineToMapReduce(seg);
        break;
      }
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
  private void combineToMapReduce(Segment combineSeg) throws Exception
  {
    CombineExpr combine = (CombineExpr) combineSeg.primaryExpr;

    Expr topParent = combine.parent();
    int topSlot = combine.getChildSlot();

    Segment mapSeg = combineSeg.firstChild;
    assert mapSeg.type == Segment.Type.INLINE_MAP;
    StReadExpr reader = (StReadExpr) mapSeg.primaryExpr;
    assert reader.isMapReducible();

    // FIXME: rewrite function should not have any parameters
    Expr input = reader.rewriteToMapReduce(new RecordExpr(Expr.NO_EXPRS)); // TODO: change name (not rewriting, but does steal inputs)
    Expr output = new HadoopTempExpr();

    // make the init expr:
    //   fn($ci) {  for $fj in <input>([$ci]) collect [ [null, $fj] ] }
    Var mapVar = engine.env.makeVar("$ci");
    Expr expr = new ArrayExpr(new VarExpr(mapVar));
    reader.replaceInParent(expr);
    BindingExpr binding = combine.binding();
    Expr combineInput = binding.inExpr(); // combine input
    Var forVar = engine.env.makeVar("$fj");
    Expr keyValPair = new ArrayExpr(new ConstExpr(Item.nil),
        new VarExpr(forVar));
    Expr forExpr = new ForExpr(forVar, combineInput, new ArrayExpr(keyValPair));
    DefineFunctionExpr initFn = new DefineFunctionExpr(null, new Var[]{mapVar},
        forExpr);

    // make the combine fn:
    //   fn($nil, $a, $b) { <usingExpr>($a,$b) }
    Var keyVar = engine.env.makeVar("$nil");
    DefineFunctionExpr combineFn = new DefineFunctionExpr(null, new Var[]{
        keyVar, binding.var, binding.var2}, combine.usingExpr());

    // make the final fn:
    //   fn($nil, $val) { [$val] }
    keyVar = engine.env.makeVar("$nil");
    Var valVar = engine.env.makeVar("$val");
    // TODO: could push upper exprs into this reduce (it is serial anyway)
    DefineFunctionExpr finalFn = new DefineFunctionExpr(null, new Var[]{keyVar,
        valVar}, new ArrayExpr(new VarExpr(valVar)));

    RecordExpr args = new RecordExpr(new Expr[]{
        new NameValueBinding("input", input),
        new NameValueBinding("output", output),
        new NameValueBinding("init", initFn),
        new NameValueBinding("combine", combineFn),
        new NameValueBinding("final", finalFn)});

    // TODO: set num reducers to one!
    MRAggregate mr = new MRAggregate(args);
    expr = new StReadExpr(mr);
    expr = new IndexExpr(expr, new ConstExpr(JLong.ZERO_ITEM)); // TODO: add firstExpr?
    topParent.setChild(topSlot, expr);

    combineSeg.root = expr;
    combineSeg.type = Segment.Type.SEQUENTIAL;
    combineSeg.firstChild = new Segment(Segment.Type.MAPREDUCE,
        combineSeg.firstChild);
    combineSeg.firstChild.root = combineSeg.firstChild.primaryExpr = mr;

    modified = true;
  }

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
    Expr                       mapValueExpr;

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
    int n = group.numInputs();

    assert reduceSeg.type == Segment.Type.FINAL_GROUP;
    for (Segment combineSeg = reduceSeg.firstChild; combineSeg != null; combineSeg = combineSeg.nextSibling)
    {
      // FINAL_GROUP with one or more COMBINE_GROUP children, each with exactly one MAP_GROUP.
      assert combineSeg.type == Segment.Type.COMBINE_GROUP;
      Segment mapSeg = combineSeg.firstChild;
      assert mapSeg.nextSibling == null; // COMBINE_GROUP has exactly one chld
      assert mapSeg.type == Segment.Type.MAP_GROUP;
      assert mapSeg.firstChild == null; // MAP_GROUP has no children
      CombineExpr combine = (CombineExpr) combineSeg.root;
      VarExpr intoVarExpr = (VarExpr) mapSeg.primaryExpr;
      Var intoVar = intoVarExpr.var();
      int i = group.getIntoIndex(intoVar);
      PerInputState inputState = inputStates[i];
      int j = inputState.combineStates.size();
      PerCombineState combineState = new PerCombineState();
      inputState.combineStates.add(combineState);

      // Copy the combine expression and its entire input tree for the map function.
      // Replace the INTO var by [ $groupIn ].
      BindingExpr inBinding = group.inBinding(i);
      Expr expr = cloneExpr(combine);
      replaceVarUses(intoVar, expr, new ArrayExpr(new VarExpr(inBinding.var)));
      combineState.init = expr;

      // Replace the combine expression in the final expression with $into[j]
      expr = new VarExpr(inBinding.var2);
      expr = new IndexExpr(expr, j);
      combine.replaceInParent(expr);

      // Make the combiner for this combine part:
      //    if isNull($a[j]) then $b[j] 
      //    else if isNull($b[j]) then $a[j]
      //    else <using>($a[j],$b[j]) else null
      expr = combine.usingExpr();
      replaceVarUses(combine.binding().var, expr, new IndexExpr(new VarExpr(
          inputState.combineIn1), j));
      replaceVarUses(combine.binding().var2, expr, new IndexExpr(new VarExpr(
          inputState.combineIn2), j));
      expr = new IfExpr(new IsnullFn(new IndexExpr(new VarExpr(
          inputState.combineIn2), j)), new IndexExpr(new VarExpr(
          inputState.combineIn1), j), expr);
      expr = new IfExpr(new IsnullFn(new IndexExpr(new VarExpr(
          inputState.combineIn1), j)), new IndexExpr(new VarExpr(
          inputState.combineIn2), j), expr);
      combineState.using = expr;
    }

    for (int i = 0; i < n; i++)
    {
      PerInputState inputState = inputStates[i];
      int m = inputState.combineStates.size();
      Expr usingExpr;
      // TODO: optimize out unnecessary arrays?  Need to fix IndexExpr references in the reducer too... 
      //      if( m == 0 )
      //      {
      //        inputState.mapValueExpr = new ConstExpr(Item.nil);
      //        usingExpr = new ConstExpr(Item.nil);
      //      }
      //      else if( m == 1 )
      //      {
      //        PerCombineState combineState = inputState.combineStates.get(0);
      //        inputState.mapValueExpr = combineState.init;
      //        usingExpr = combineState.using;
      //      }
      //      else
      {
        Expr[] inits = new Expr[m];
        Expr[] usings = new Expr[m];
        for (int j = 0; j < m; j++)
        {
          PerCombineState combineState = inputState.combineStates.get(j);
          inits[j] = combineState.init;
          usings[j] = combineState.using;
        }
        inputState.mapValueExpr = new ArrayExpr(inits);
        usingExpr = new ArrayExpr(usings);
      }
      combineFns[i] = new DefineFunctionExpr(null, new Var[]{
          inputState.combineKey, inputState.combineIn1, inputState.combineIn2},
          usingExpr);
    }
  }

  /**
   * @param groupSeg
   */
  private void groupToMapReduce(Segment groupSeg)
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
      BindingExpr inBinding = (BindingExpr) group.inBinding(i);
      PerInputState inputState = inputStates[i] = new PerInputState(engine.env,
          i);

      // Replace the reader with [ $mapIn ]
      StReadExpr reader = (StReadExpr) mapSeg.primaryExpr;
      assert reader.isMapReducible();
      inputs[i] = reader.rewriteToMapReduce(new RecordExpr(Expr.NO_EXPRS)); // TODO: change name (not rewriting, but does steal inputs)
      Expr expr = new ArrayExpr(new VarExpr(inputState.mapIn));
      reader.replaceInParent(expr);

      // Build the initial map expression
      inputStates[i].mapValueExpr = new VarExpr(inBinding.var);

      // Use the group into variables for the reduce/final function
      reduceParams[i + 1] = inBinding.var2; // intoVar

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
      PerInputState inputState = inputStates[i];
      BindingExpr inBinding = group.inBinding(i);
      Expr byExpr = byBinding.byExpr(i);
      Expr keyValPair = new ArrayExpr(byExpr, inputState.mapValueExpr);
      Expr forExpr = new ForExpr(inBinding.var, inBinding.inExpr(),
          new ArrayExpr(keyValPair));
      mapFns[i] = new DefineFunctionExpr(null, new Var[]{inputState.mapIn},
          forExpr);
    }

    // Make the output
    Expr output;
    Expr lastExpr = groupSeg.root;
    boolean writing = lastExpr instanceof StWriteExpr;
    if (writing)
    {
      StWriteExpr writer = (StWriteExpr) groupSeg.root;
      assert writer.isMapReducible();
      // FIXME: rewrite function should not have any parameters
      output = writer.rewriteToMapReduce(new RecordExpr(Expr.NO_EXPRS)); // TODO: change name (not rewriting, but does steal inputs)
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
      expr = new StReadExpr(mr);
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

    StReadExpr reader = (StReadExpr) mapSeg.primaryExpr;
    assert reader.isMapReducible();

    if (mapSeg.root == reader)
    {
      // read(T) is marked as a map segment, but it is silly to run it as map/reduce.
      // It probably shouldn't be marked as a map segment...
      return;
    }

    Expr topParent = mapSeg.root.parent();
    int topSlot = mapSeg.root.getChildSlot();

    Expr input = reader.rewriteToMapReduce(new RecordExpr(Expr.NO_EXPRS)); // TODO: change name (not rewriting, but does steal inputs)
    Var mapIn = engine.env.makeVar("$mapIn");
    Expr expr = new ArrayExpr(new VarExpr(mapIn));
    reader.replaceInParent(expr);

    // Make the output
    Expr output;
    Expr lastExpr = mapSeg.root;
    boolean writing = lastExpr instanceof StWriteExpr;
    if (writing)
    {
      StWriteExpr writer = (StWriteExpr) mapSeg.root;
      assert writer.isMapReducible();
      // FIXME: rewrite function should not have any parameters
      output = writer.rewriteToMapReduce(new RecordExpr(Expr.NO_EXPRS)); // TODO: change name (not rewriting, but does steal inputs)
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
      expr = new StReadExpr(expr);
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

    if (expr instanceof ForExpr)
    {
      ForExpr forExpr = (ForExpr) expr;
      seg = segment(forExpr.binding().inExpr()); // For input
      switch (seg.type)
      {
        case SEQUENTIAL :
          // the input is running sequentially,
          // -> the for loop will run sequentially, 
          // -> use the same segment
          // try to parallelize the return
          seg.addChild(segment(forExpr.collectExpr()));
          break;
        case MAP :
        case GROUP :
          if (mightContainMapReduce(forExpr.collectExpr())) // FIXME: this needs to look for other functions that cannot be relocated (eg, local read/write)
          {
            seg = new Segment(Segment.Type.SEQUENTIAL, seg);
          }
          break;
        default :
          seg = new Segment(Segment.Type.SEQUENTIAL, seg);
      }
    }
    else if (expr instanceof LetExpr)
    {
      // Lets are always run sequentially
      LetExpr let = (LetExpr) expr;
      seg = new Segment(Segment.Type.SEQUENTIAL);
      int n = let.numBindings();
      for (int i = 0; i < n; i++)
      {
        seg.addChild(segment(let.binding(i).eqExpr()));
      }
      seg.addChild(segment(let.returnExpr()));
    }
    else if (expr instanceof GroupByExpr)
    {
      GroupByExpr group = (GroupByExpr) expr;
      seg = new Segment(Segment.Type.GROUP);
      seg.primaryExpr = group;
      int n = group.numInputs();
      for (int i = 0; i < n; i++)
      {
        Segment s = segment(group.inBinding(i).inExpr());
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
    else if (expr instanceof CombineExpr)
    {
      CombineExpr combine = (CombineExpr) expr;
      Segment s = segment(combine.binding().inExpr());
      if (s.type == Segment.Type.GROUP || s.type == Segment.Type.COMBINE)
      {
        s = makeMapSegment(s);
      }
      if (s.type == Segment.Type.MAP)
      {
        s.type = Segment.Type.INLINE_MAP;
        seg = new Segment(Segment.Type.COMBINE, s);
        seg.primaryExpr = combine;
      }
      else if (s.type == Segment.Type.SEQUENTIAL)
      {
        seg = s;
      }
      else
      {
        seg = new Segment(Segment.Type.SEQUENTIAL, s);
      }
    }
    else if (expr instanceof StReadExpr)
    {
      StReadExpr reader = (StReadExpr) expr;
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
    else if (expr instanceof StWriteExpr)
    {
      StWriteExpr writer = (StWriteExpr) expr;
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
    Expr tmp = new StWriteExpr(new HadoopTempExpr(), root);
    Expr read = new StReadExpr(tmp);
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
      else if (segmentReduceIsLocal(group, expr))
      {
        seg = new Segment(Segment.Type.FINAL_GROUP);
      }
      else
      {
        seg = new Segment(Segment.Type.SEQUENTIAL_GROUP);
      }
    }
    else if (expr instanceof ForExpr)
    {
      ForExpr fe = (ForExpr) expr;
      if (segmentReduceIsLocal(group, fe.collectExpr()))
      {
        seg = segmentReduceCombine(group, fe.binding().inExpr());
      }
      else
      {
        seg = new Segment(Segment.Type.SEQUENTIAL_GROUP);
      }
    }
    else if (expr instanceof CombineExpr) // chained combined expression - push into map phase
    {
      CombineExpr combine = (CombineExpr) expr;
      seg = segmentReduceCombine(group, combine.binding().inExpr());
    }
    else
    {
      if (segmentReduceIsLocal(group, expr))
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
   * Returns a Segment with type: FINAL_GROUP: not good for a combiner, but ok
   * with other combiners This may have *zero or more* COMBINE_GROUP children
   * which each have exactly one MAP_GROUP child SEQUENTIAL_GROUP: not good with
   * any combiners which has no children
   * 
   * @param group
   * @param expr
   * @return
   */
  protected Segment segmentReduce(GroupByExpr group, Expr expr)
  {
    Segment seg;
    // FIXME: for-macros (unnest, denull, deempty, ...)
    // FIXME: let on combine input
    if (expr instanceof CombineExpr)
    {
      CombineExpr combine = (CombineExpr) expr;
      seg = segmentReduceCombine(group, combine.binding().inExpr());
      if (seg.type == Segment.Type.MAP_GROUP)
      {
        if (segmentReduceIsLocal(group, combine.usingExpr()))
        {
          seg = new Segment(Segment.Type.COMBINE_GROUP, seg);
          seg.root = seg.primaryExpr = combine;
          seg = new Segment(Segment.Type.FINAL_GROUP, seg);
        }
        else
        {
          // fields other than the key and 
          seg.type = Segment.Type.SEQUENTIAL_GROUP;
          seg.firstChild = null;
        }
      }
      else
      {
        assert seg.type == Segment.Type.SEQUENTIAL_GROUP;
      }
    }
    else if (expr instanceof VarExpr)
    {
      VarExpr ve = (VarExpr) expr;
      if (group.getIntoIndex(ve.var()) >= 0)
      {
        // There is a reference to an into var outside of a combiner, so no combining
        seg = new Segment(Segment.Type.SEQUENTIAL_GROUP);
      }
      else
      {
        seg = new Segment(Segment.Type.FINAL_GROUP);
      }
    }
    else
    {
      // TODO: need to look for anything else here?
      if (expr.numChildren() == 0)
      {
        seg = new Segment(Segment.Type.FINAL_GROUP);
      }
      else
      {
        seg = null;
        for (Expr e : expr.children())
        {
          Segment s = segmentReduce(group, e);
          if (s.type == Segment.Type.SEQUENTIAL_GROUP)
          {
            seg = s;
            break;
          }
          else
          {
            assert s.type == Segment.Type.FINAL_GROUP;
            if (seg == null)
            {
              seg = s;
            }
            else
            {
              // merge the FINAL_GROUP segments by adopting the children
              seg.addChild(s.firstChild);
            }
          }
        }
      }
    }

    seg.root = expr;
    return seg;
  }

}

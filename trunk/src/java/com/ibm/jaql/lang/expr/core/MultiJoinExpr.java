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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.nil.IsnullExpr;
import com.ibm.jaql.lang.expr.path.PathFieldValue;

/**
 * MultiJoin
 *   |- Binding in1 |- inPipe
 *       |> Binding key1=onExpr
 *   |- Binding in2 |- inPipe
 *       |- Binding key1 |> onExpr
 *       |> Binding key2 |> onExpr
 *   |- Binding in3 |- inPipe
 *       |> Binding key2 |> onExpr
 *   |> projectExpr
 *   
 * MultiJoin is a macro for a number of joins plus some filters. Eventually, we will
 * make this a LogicalExpr that flows through the rewriter/optimizer so we can do join
 * order optimization.
 * 
 * A MultiJoin can describe an arbitrary connected equijoin graph.  It allows some of its inputs
 * to be "preserved" which causes outer joins to be produced in a consistent and predictable way.
 * When no input is preserved, the join is completely an inner join and supports an 
 * arbitrary connected equijoin graph. When one or more inputs are preserved, the join graph
 * is restricted:
 *    * If there is only one variable connecting all the inputs, then a simple n-way join on that
 *      one key is produced.
 *    * Otherwise, the join graph must be acyclic.
 *     
 *   Examples:
 *   * This is the trouble-maker for cycles with preserved.  I don't think it has a clear meaning.
 *     and certainly is not expressible as binary outer joins (without breaking the cycle and repeating one input).
 *          
 *         join preserved $x on $a = $x.a on $b = $x.b, 
 *              preserved $y on $a = $y.a on $c = $y.c,
 *              preserved $z on $a = $z.a on $c = $z.c 
 *              
 *     This is the join graph produced:
 *     
 *       x <-> y <->z
 *       ^  a     c ^
 *       |----------|
 *            b
 *            
 *   * All on one var is ok:
 *         join preserved $x on $a = $x.a, $y on $a = $y.a, $z on $a = $z.a 
 *   * This could be ok, but is error for now
 *         join preserved $x on $a = $x.a on $b = $x.b, $y on $a = $y.a on $b = $y.b, $z on $a = $z.a on $b = $z.b 
 *   * This is ok, and equivalent to previous
 *         join preserved $x on $c = [$x.a, $x.b], $y on $c = [$y.a, $y.b], $z on $c = [$z.a, $z.b]
 *   * This could be ok, but is error for now 
 *         join preserved $w on $a = $w.a, $x on $a = $x.a on $b = $x.b, $y on $b = $y.b, $z on $b = $z.b 
 *   * This is ok, and eqv to previous
 *         join preserved $w on $a = $w.a, $x on $a = $x.a on $b = $x.b, $y on $b = $y.b on $b2 = $y.b, $z on $b2 = $z.b 
 */
public class MultiJoinExpr extends MacroExpr
{
  /**
   * 
   * @param bindings
   * @param where
   * @param expand
   * @return
   */
  private static Expr[] makeExprs(ArrayList<BindingExpr> bindings, Expr where, Expr expand)
  {
    int n = bindings.size();
    Expr[] exprs = new Expr[n + 2];
    for (int i = 0; i < n; i++)
    {
      BindingExpr b = bindings.get(i);
      assert b.type == BindingExpr.Type.IN;
      exprs[i] = b;
    }
    exprs[n] = where;
    exprs[n+1]   = expand;
    return exprs;
  }

  /**
   * @param exprs
   */
  public MultiJoinExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param bindings
   * @param returnExpr
   */
  public MultiJoinExpr(ArrayList<BindingExpr> bindings, Expr where, Expr expand)
  {
    super(makeExprs(bindings, where, expand));
  }
  
  /**
   * @return
   */
  public int numBindings()
  {
    return exprs.length - 2;
  }

  /**
   * @param i
   * @return
   */
  public BindingExpr binding(int i)
  {
    assert i < exprs.length - 2;
    return (BindingExpr) exprs[i];
  }

  /**
   * @return
   */
  public Expr whereExpr()
  {
    return exprs[exprs.length - 2];
  }

  /**
   * @return
   */
  public Expr projectExpr()
  {
    return exprs[exprs.length - 1];
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    // TODO: fix this
    exprText.print(kw("join") + " ");
    int n = numBindings();
    String sep = "";
    for (int i = 0; i < n; i++)
    {
      exprText.print(sep);
      BindingExpr b = binding(i);
      if( b.preserve )
      {
        exprText.print(kw("preserved") + " ");
      }
      exprText.print(b.var.taggedName());
      exprText.print(" " + kw("in") + " (");
      b.inExpr().decompile(exprText, capturedVars);
      exprText.print(")");
      int m = b.numChildren();
      for(int j = 1 ; j < m ; j++)
      {
        BindingExpr on = (BindingExpr)b.child(i);
        exprText.print(" " + kw("on") + " ");
        exprText.print(on.var.taggedName());
        exprText.print(" = ");
        b.eqExpr().decompile(exprText, capturedVars);
      }
      b.inExpr().decompile(exprText, capturedVars);
      sep = ",\n     ";
    }
    exprText.println(" " + kw("into") + " ");
    projectExpr().decompile(exprText, capturedVars);

    for (int i = 0; i < n; i++)
    {
      BindingExpr b = binding(i);
      capturedVars.remove(b.var);
      int m = b.numChildren() - 1;
      for(int j = 1 ; j < m ; j++)
      {
        BindingExpr on = (BindingExpr)b.child(i);
        capturedVars.remove(on.var);
      }
    }
  }

  public Expr expand(Env env) // throws Exception
  {
    // Build the basic join graph from the join key variables
    // Raise error if the join graph is not connected 
    //    We block implict cross products
    //    A cross product is still possible using key=const
    // If any input is marked preserved, add outer-join annotations to the join graph edges
    //    We start with an inner-join graph and some inputs that are marked preserved.
    //    Some notation:
    //         (x -- y) means (x innerJoin y)
    //         (x -> y) means (x rightOuterJoin y), ie x is preserved and y is null-producing
    //         (x <-> y) means (x fullOuterJoin y)
    //         (x ?-> y) means (x rightOuterJoin y) or (x fullOuterJoin y)
    //    for each preserved input x, make x |-> y
    //    Extend outer joins away from x: compute the closure of the following: 
    //      for each node y and any node z != x such that x ?-> y ?-? z, make it x ?-> y ?-> z
    //         This implies x ?-> y <-? z becomes x <-> y <-> z
    //         Moreover, any edge on a path between two preserved nodes will be full outer joins and
    //            any other edges are left/right outer joins.
    //         Therefore, we have a full outer join core and left/right outer join spurs, eg for:
    //                 a -- [x] -- y -- [z] -- b
    //            with x & z marked preserved we get:
    //                 a <-| ([x] <-> y <-> [z] ) |-> b
    //         This also includes any edges along a cycle that includes a preserved node:
    //              [x] ---- y  ==>   [x] <----> y
    //               |       |  ==>    |         |
    //               |-- c --|         |<-> c <->|
    //
    //    For the axyzb chain query, this procedure generates potential outputs including:  
    //            -x---, -xy--, --yz--, and ---z- (where - is null for that input).
    //      But we could get some result that is --y--, because y is also in the core.
    //      We don't want that result because y is not marked preserved.
    //      So to eliminate these results we add a filter that accepts any tuple that has 
    //        a non-null value for at least one of the preserved inputs.
    //    The result of this procedure is a parital plan:
    //        full outer join core -> filter some preserved not null -> left outer spurs, eg:
    //            join preserve x, preserve y 
    //            -> join preserve in, preserve z
    //            -> filter not isnull(x) or not isnull(z)
    //            -> join preserve in, a
    //            -> join preserve in, b
    
    // TODO: when join is on a single variable, generate a single join.
    
    JoinGraph graph = new JoinGraph(env);    
    Expr expand = projectExpr();
    Var vjoined = env.makeVar("$");
    VarMap varMap = new VarMap();
    
    int n = numBindings();
    Expr pipe = graph.inputs[0].mappedBinding;
    Var vold = graph.inputs[0].binding.var;
    Expr path = PathFieldValue.byVarName(vjoined, vold);
    expand = expand.replaceVarUses(vold, path, varMap);

    for( int i = 1 ; i < n ; i++ )
    {
      pipe = graph.makeJoin(env, i, pipe, varMap);
      vold = graph.inputs[i].binding.var;
      path = PathFieldValue.byVarName(vjoined, vold);
      expand = expand.replaceVarUses(vold, path, varMap);
    }
    
    pipe = new ForExpr(vjoined, pipe, expand);
    
    return pipe;
  }


  static final class JoinInput
  {
    int visited = 0;
    final int bindingId; // the index of this input in the MultiJoin
    final BindingExpr binding;
    Expr mappedBinding;  //   <bind.expr> -> map { bindVar: $ }
    final ArrayList<JoinInfo> joins = new ArrayList<JoinInfo>();
    public int orderedId; // the index of this input in the JoinGraph after planning
    boolean inCore = false; // true if preserved or on a path between two preserved inputs (and therefore, outer-joined)

    
    public JoinInput(Env env, int bindingId, BindingExpr binding)
    {
      this.bindingId = bindingId;
      this.binding = binding;
      
      // mappedBinding ::= <input> -> map each $inVar { $inVar } 
      Expr[] fields = new Expr[1];
      fields[0] = new NameValueBinding(binding.var, false);
      this.mappedBinding = new TransformExpr(binding.var, binding.inExpr(), new RecordExpr(fields));
    }

    public JoinInfo findJoin(JoinInput y)
    {
      for( JoinInfo join: joins )
      {
        if( join.input1 == y || join.input2 == y )
        {
          return join;
        }
      }
      return null;
    }

    public boolean isPreserved()
    {
      return binding.preserve;
    }

    public boolean isFullOuterJoined()
    {
      for( JoinInfo join: joins )
      {
        if( join.type == JoinType.FULL_OUTER )
        {
          return true;
        }
      }
      return false;
    }
  }
  
  enum JoinType
  {
    INNER,         // input1 --- input2
    SEMI_OUTER,    // input1 --> input2
    FULL_OUTER;    // input1 <-> input2
  }
  
  static final class JoinInfo
  {
    JoinType type = JoinType.INNER;
    JoinInput input1;
    JoinInput input2;
    ArrayList<Expr> expr1 = new ArrayList<Expr>();
    ArrayList<Expr> expr2 = new ArrayList<Expr>();
    
    public int visited;
    
    public JoinInfo(JoinInput x, Expr ex, JoinInput y, Expr ey)
    {
      assert x != y;
      input1 = x;
      input2 = y;
      expr1.add(ex);
      expr2.add(ey);
      x.joins.add(this);
      y.joins.add(this);
      if( x.isPreserved() )
      {
        setOuter(x,y);
      }
      if( y.isPreserved() )
      {
        setOuter(y,x);
      }
    }

    public void add(Expr ex, Expr ey)
    {
      expr1.add(ex);
      expr2.add(ey);
    }

    public boolean isOuter()
    {
      return type != JoinType.INNER;
    }
    
    public JoinInput getOther(JoinInput x)
    {
      if( input1 == x )
      {
        return input2;
      }
      assert input2 == x;
      return input1;
    }

    /**
     * 
     * @param x
     * @param y
     * @return true iff x ?-> y (ie, x --> y or x <-> y )
     */
    public boolean isOuter(JoinInput x, JoinInput y)
    {
      return type == JoinType.FULL_OUTER || 
             type == JoinType.SEMI_OUTER && input1 == x && input2 == y;  
    }
    
    /**
     * make x ?-- y into x --> y
     *  ie, x --- y into x --> y
     *      x <-- y into x <-> y
     *      x --> y no change
     *      x <-> y no change
     * @param x
     * @param y
     */
    public void setOuter(JoinInput x, JoinInput y)
    {
      assert (input1 == x && input2 == y) || (input1 == y && input2 == x);
      if( type == JoinType.INNER )
      {
        if( input2 == x )
        {
          input1 = x;
          input2 = y;
          ArrayList<Expr> es = expr1; 
          expr1 = expr2; 
          expr2 = es;
        }
        type = JoinType.SEMI_OUTER;
      }
      else if( type == JoinType.SEMI_OUTER && input1 == y )
      {
        type = JoinType.FULL_OUTER;
      }
    }
    
    public String toString()
    {
      StringBuilder out = new StringBuilder();
      String s = "eek";
      switch( type )
      {
        case INNER:       s = " --- "; break;
        case SEMI_OUTER:  s = " --> "; break;
        case FULL_OUTER:  s = " <-> "; break;
      }
      if( this.input1.isPreserved() ) out.append("[");
      out.append(this.input1.binding.var.taggedName());
      if( this.input1.isPreserved() ) out.append("]");
      out.append(s);
      if( this.input2.isPreserved() ) out.append("[");
      out.append(this.input2.binding.var.taggedName());
      if( this.input2.isPreserved() ) out.append("]");
      s = " on ";
      int n = expr1.size();
      for(int i = 0 ; i < n ; i++)
      {
        out.append(s);
        out.append(expr1.get(i));
        out.append("==");
        out.append(expr2.get(i));
        s = ", ";
      }
      return out.toString();
    }
  }

  
  private class JoinGraph
  {
    int clock = 1; // visit clock

    int numPreserved = 0; // number of preserved inputs
    int numInCore = 0;    // number of inputs 
    
    JoinInput[] inputs;
    
    /**
     * Map each distinct variable to the set of input that join on that var.
     */
    HashMap<Var,ArrayList<JoinInput>> varToInputs = new HashMap<Var,ArrayList<JoinInput>>();

    ArrayList<JoinInfo> edges = new ArrayList<JoinInfo>();
    
    JoinGraph(Env env)
    {
      makeInputs(env);
      makeEdges(whereExpr());
      JoinInput x = findDisconnected();
      if( x != null )
      {
        throw new RuntimeException("join must be fully connected.  For example, this is not connected to the first input: "+x.binding.var.taggedName());
      }
      if( numPreserved > 0 )
      {
        if( isCyclic() )
        {
          throw new RuntimeException("preserving joins must be acyclic (tree/star/snowflake) or on a single variable");
        }
        propagateOuterJoins();
        determineCore();
      }
      reorderInputs();
    }

    private boolean isCyclic()
    {
      clock++;
      return isCyclic(inputs[0]);
    }
    
    private boolean isCyclic(JoinInput x)
    {
      x.visited = clock;
      for( JoinInfo join: x.joins )
      {
        if( join.visited < clock )
        {
          join.visited = clock;
          JoinInput y = join.getOther(x);
          if( y.visited >= clock )
          {
            return true;
          }
          if( isCyclic(y) )
          {
            return true;
          }
        }
      }
      return false;
    }

    public Expr makeJoin(Env env, int startAt, Expr pipe, VarMap varMap)
    {
      // --------------------------
      // Make the input vars
      // --------------------------
      JoinInput base = inputs[startAt];
      Var pipeVar = env.makeVar("$a");  
      Var baseVar = env.makeVar("$b");      

      // --------------------------
      // Get the eligible join predicates:
      //   A join is eligible if it connects with an earlier input.
      //   We know an input is already joined in if its clock is 
      // --------------------------
      assert base.orderedId == startAt;
      ArrayList<JoinInfo> eligible = new ArrayList<JoinInfo>();
      for( JoinInfo join: base.joins )
      {
        if( join.getOther(base).orderedId < base.orderedId )
        {
          eligible.add(join);
        }
      }

      // --------------------------
      // Make the join conditions
      // --------------------------
      Expr onPipe;
      Expr onBase;
      if( eligible.size() == 1 && eligible.get(0).expr1.size() == 1 )
      {
        // One eligible join on one key, no need for array on join key
        onPipe = makeKey(eligible.get(0), base, baseVar, pipeVar, 0, varMap);
        onBase = makeKey(eligible.get(0), base, baseVar, baseVar, 0, varMap);
      }
      else
      {
        // multiple join conditions: make array of conditions
        ArrayList<Expr> pipeKey = new ArrayList<Expr>();
        ArrayList<Expr> baseKey = new ArrayList<Expr>();
        for( JoinInfo join: eligible )
        {
          int n = join.expr1.size();
          for( int i = 0 ; i < n ; i++ )
          {
            pipeKey.add( makeKey(join, base, baseVar, pipeVar, i, varMap) );
            baseKey.add( makeKey(join, base, baseVar, baseVar, i, varMap) );
          }
        }
        onPipe = new ArrayExpr(pipeKey);
        onBase = new ArrayExpr(baseKey);
      }

      // --------------------------
      // Create the output record: { $pipe.*, $base.* }
      // --------------------------
      Expr project = new RecordExpr(
          new CopyRecord(new VarExpr(pipeVar)), 
          new CopyRecord(new VarExpr(baseVar)) );
      
      // --------------------------
      // Create the join
      // --------------------------
      Expr[] joinArgs = new Expr[] {
          new BindingExpr(BindingExpr.Type.IN, pipeVar, null, numPreserved > 0, pipe),
          onPipe,
          new BindingExpr(BindingExpr.Type.IN, baseVar, null, base.inCore, base.mappedBinding),
          onBase,
          new ArrayExpr(project)
      };
      pipe = new JoinExpr(joinArgs);
      
      // --------------------------
      // If we are done with the full-outer core and the core has non-preserved inputs 
      // add filter some preserved is non-null
      // --------------------------
      if( startAt + 1 == numInCore && numPreserved < numInCore )
      {
        Var filterVar = env.makeVar("$");
        Expr pred = null;
        for(int i = 0 ; i < numInCore ; i++)
        {
          JoinInput in = inputs[i];
          if( in.isPreserved() )
          {
            Expr e= PathFieldValue.byVarName(filterVar, in.binding.var);
            e = new NotExpr(new IsnullExpr(e));
            if( pred == null )
            {
              pred = e;
            }
            else
            {
              pred = new OrExpr(pred, e);
            }
          }
        }
        pipe = new FilterExpr(filterVar, pipe, pred);
      }
      
      return pipe; 
    }

    private Expr makeKey(JoinInfo join, JoinInput base, Var baseVar, Var vin, int exprId, VarMap varMap)
    {
      Var vold;
      Expr e;
      if( (join.input1 == base) ^ (vin == baseVar) )
      {
        vold = join.input2.binding.var;
        e = join.expr2.get(exprId);
      }
      else
      {
        vold = join.input1.binding.var;
        e = join.expr1.get(exprId);
      }
      // replace $vold in e with $vin.vold
      Expr path = PathFieldValue.byVarName(vin, vold);
      e = e.replaceVarUses(vold, path, varMap);
      return e;
    }

    private void makeInputs(Env env)
    {
      int n = numBindings();
      inputs = new JoinInput[n];
      numPreserved = 0;
      for(int i = 0 ; i < n ; i++)
      {
        BindingExpr b = binding(i);
        JoinInput in = inputs[i] = new JoinInput(env, i, b);
        if( in.isPreserved() )
        {
          numPreserved++;
        }
        int m = b.numChildren();
        for(int j = 1 ; j < m ; j++)
        {
          BindingExpr on = (BindingExpr)b.child(j);
          ArrayList<JoinInput> varInputs = varToInputs.get(on.var);
          if( varInputs == null )
          {
            varInputs = new ArrayList<JoinInput>();
            varToInputs.put(on.var, varInputs);
          }
          varInputs.add(in);
        }
      }
    }

    private JoinInput findInput(Var v)
    {
      for(JoinInput i: inputs)
      {
        if( i.binding.var == v )
        {
          return i;
        }
      }
      return null;
    }
    
    private void addEdge(JoinInput x, Expr ex, JoinInput y, Expr ey)
    {
      JoinInfo join = x.findJoin(y);
      if( join == null )
      {
        join = new JoinInfo(x,ex,y,ey);
        edges.add(join);
      }
      else if( join.input1 == x )
      {
        join.add(ex,ey);
      }
      else 
      {
        assert join.input2 == x;
        join.add(ey,ex);
      }
    }
    
    private void makeEdges(Expr pred)
    {
      if( pred instanceof CompareExpr )
      {
        CompareExpr c = (CompareExpr)pred;
        if( c.op != CompareExpr.EQ )
        {
          throw new RuntimeException("Only equality predicates are supported by join at this time");
        }
        Expr e1 = c.child(0);
        Expr e2 = c.child(1);
        HashSet<Var> s1 = e1.getCapturedVars();
        HashSet<Var> s2 = e2.getCapturedVars();
        if( s1.size() == 1 && s2.size() == 1 )
        {
          Var v1 = s1.iterator().next();
          Var v2 = s2.iterator().next();
          JoinInput x = findInput(v1);
          JoinInput y = findInput(v2);
          if( x == null || y == null )
          {
            throw new RuntimeException("Only predicates on join inputs are supported by join at this time");
          }
          if( x == y )
          {
            throw new RuntimeException("Only predicates on two different join inputs are supported by join at this time");
          }
          addEdge(x,e1,y,e2);
        }
        else
        {
          throw new RuntimeException("Only predicates on two join inputs are supported by join at this time");
        }
      }
      else if( pred instanceof AndExpr )
      {
        makeEdges(pred.child(0));
        makeEdges(pred.child(1));
      }
    }
    
//    private void makeJoins()
//    {
//      for( Map.Entry<Var,ArrayList<JoinInput>> entry: varToInputs.entrySet() )
//      {
//        Var var = entry.getKey();
//        for( JoinInput x: entry.getValue() )
//        {
//          for( JoinInput y: entry.getValue() )
//          {
//            if( x.bindingId < y.bindingId )
//            {
//              JoinInfo join = x.findJoin(y);
//              if( join == null )
//              {
//                join = new JoinInfo(var,x,y);
//                edges.add(join);
//              }
//              else
//              {
//                join.add(var);
//              }
//            }
//          }
//        }
//      }
//    }
    
    /**
     * If the join graph is fully connected (ie, a join path from every pair of nodes), return null.
     * Otherwise, return any input that is not connected to the first input.
     * 
     * @return
     */
    public JoinInput findDisconnected()
    {
      clock++;
      visitConnected(inputs[0]);
      for( JoinInput x: inputs )
      {
        if( x.visited < clock )
        {
          return x;
        }
      }
      return null;
    }
    
    /**
     * Recursively visit all nodes reachable from input x.
     * 
     * @param x
     */
    private void visitConnected(JoinInput x)
    {
      x.visited = clock;
      for( JoinInfo join: x.joins )
      {
        JoinInput y = join.getOther(x);
        if( y.visited < clock )
        {
          visitConnected(y);
        }
      }
    }
    

    /**
     * Produce the closure of this:
     * When    x ?-> y ?-- z, x != z
     * make it x ?-> y ?-> z
     * 
     * @param y
     * @return
     */
    private void propagateOuterJoins()
    {
      boolean progress;
      do
      {
        progress = false;
        for( JoinInfo join: edges )
        {
          JoinInput x = join.input1;
          JoinInput y = join.input2;
          if( join.isOuter(x,y) )   // found x ?-> y
          {
            for( JoinInfo join2: y.joins )
            {
              JoinInput z = join2.getOther(y);
              if( x != z && ! join2.isOuter(y,z) ) // found x != z, y ?-- z
              {
                join2.setOuter(y, z); // make it y ?-> z
                progress = true;
              }
            }
          }
        }
      } while( progress );
    }

    /**
     * Mark and count all the inputs that are part of the full-outer join core.
     */
    private void determineCore()
    {
      numInCore = 0;
      for( JoinInput x: inputs )
      {
        if( x.isFullOuterJoined() )
        {
          x.inCore = true;
          numInCore++;
        }
      }
    }
    
    /**
     * Reorder the join inputs such that:
     *   Required:
     *     - For any input y except the first, y is connected to some x < y.
     *   Optional:
     *     TBD if two or more joins have the same key set, they are contiguous in to order
     *        (This lets us turn them into one n-ary join)
     *     - If preserving:
     *       - some preserved input is first
     *       - all full outers are placed before all semi-outers
     *     - Else is inner join:
     *       - the first input is left there.
     *     
     */
    private void reorderInputs()
    {
      final int n = inputs.length;
      HashSet<JoinInput> done = new HashSet<JoinInput>(n);
      HashSet<JoinInput> ready = new HashSet<JoinInput>(n);

      if( numPreserved > 0 )
      {
        // Find a preserved input and make it go first.
        int i;
        for( i = 0 ; i < n ; i++ )
        {
          if( inputs[i].isPreserved() )
          {
            JoinInput x = inputs[i];
            inputs[i] = inputs[0];
            inputs[0] = x;
            break;
          }
        }
        assert i < n; // a preserved input must be found or numPreserved == 0
      }

      JoinInput prevInput = inputs[0];
      prevInput.orderedId = 0;
      done.add(prevInput);
      for( JoinInfo join: inputs[0].joins )
      {
        JoinInput y = join.getOther(prevInput);
        ready.add(y);
      }
      
      int i = 1;
      while( ! ready.isEmpty() )
      {
        JoinInput x = pickBestInput(ready);
        ready.remove(x);
        inputs[i] = x;
        x.orderedId = i;
        i++;
        done.add(x);
        prevInput = x;
        for( JoinInfo join: x.joins )
        {
          JoinInput y = join.getOther(x);
          if( ! done.contains(y) )
          {
            ready.add(y);
          }
        }
      }
      assert i == n; // we should have processed them all
    }

    
    private JoinInput pickBestInput(HashSet<JoinInput> ready)
    {
      JoinInput best = null;
      for( JoinInput x: ready )
      {
// This requires us to reason about joins instead of inputs.
// It is non-trival: a on w,v; b on w,v; c on w; d on v
//   Join a,b,c on w -> filter b.v=a.v -> join in,d on v        
//        if( prevInput.joinsOnSameKey(x) )
//        {
//          return x;
//        }
        // The check on bindingId below is to get a deterministic order on joins from query to query.
        if( best == null || 
            ( x.inCore && ! best.inCore ) ||
            ( x.inCore == best.inCore && x.bindingId < best.bindingId ))
        {
          best = x;
        }
      }
      return best;
    }
    
    public String toString()
    {
      StringBuilder out = new StringBuilder();
      for( JoinInfo join: this.edges )
      {
        out.append(join.toString());
        out.append("\n");
      }
      return out.toString();
    }
  }

}

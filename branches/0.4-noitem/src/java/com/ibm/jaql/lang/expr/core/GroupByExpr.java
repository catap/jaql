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
package com.ibm.jaql.lang.expr.core;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.util.ItemHashtable;
import com.ibm.jaql.util.Bool3;

// TODO: translate cogroup into group over merge?
// group $x by e1<$x>, $y by e2<$y> into e3<$by,$x,$y>
// ==
// merge |- $x -> project { by: e1<$x>, x:$x ] 
//       |> $y -> project { by: e2<$y>, y:$y ] 
// -> group $in by $in.by into e3<$key,$x=$in[*].x,$y=$in[*].y>>

/**
 * 
 */
public class GroupByExpr extends IterExpr
{
  /**
   * 
   * @param exprs
   * @param as  Each item must be either BindingExpr or Var
   * @return
   */
  private static Expr[] addAsVars(Expr[] exprs, ArrayList<?> as)
  {
    int n = as.size();
    assert exprs.length == n + 4;
    for(int i = 0 ; i < n ; i++)
    {
      Object x = as.get(i);
      BindingExpr b;
      if( x instanceof Var )
      {
        b = new BindingExpr(BindingExpr.Type.EQ,(Var)x,null,Expr.NO_EXPRS);
      }
      else
      {
        b = (BindingExpr)x;
      }
      assert b.numChildren() == 0; 
      exprs[i+2] = b;
    }
    return exprs;
  }
  
  /**
   * 
   * @param exprs
   * @param as
   * @return
   */
  private static Expr[] addAsVars(Expr[] exprs, Var[] as)
  {
    assert exprs.length == as.length + 4;
    for(int i = 0 ; i < as.length ; i++)
    {
      exprs[i+2] = new BindingExpr(BindingExpr.Type.EQ,as[i],null,Expr.NO_EXPRS);
    }
    return exprs;
  }
  
  private static Expr[] makeExprs(
      BindingExpr in, 
      BindingExpr by, 
      Expr using, 
      Expr expand)
  {
    int n = in.numChildren();
    assert n == by.numChildren();
    Expr[] exprs = new Expr[n + 4];
    exprs[0] = in;
    exprs[1] = by;
    if( using == null )
    {
      using = new ConstExpr(Item.NIL);
    }
    exprs[n+2] = using;
    exprs[n+3] = expand;
    return exprs;
  }

  private static Expr[] makeExprs(
      BindingExpr in, 
      BindingExpr by, 
      BindingExpr as, 
      Expr using, 
      Expr expand)
  {
    assert 1 == in.numChildren();
    assert 1 == by.numChildren();
    if( using == null )
    {
      using = new ConstExpr(Item.NIL);
    }
    return new Expr[]{ in, by, as, using, expand };
  }

//  /**
//   * Note: the byBindings are at bindings.get(0), but moved to
//   * exprs[exprs.length-2].
//   * 
//   * @param bindings
//   * @param doExpr
//   * @return
//   */
//  private static Expr[] makeExprs(ArrayList<BindingExpr> bindings, Expr doExpr)
//  {
//    int n = bindings.size();
//    Expr[] exprs = new Expr[n + 1];
//    for (int i = 1; i < n; i++)
//    {
//      BindingExpr b = bindings.get(i);
//      assert b.type == BindingExpr.Type.IN;
//      exprs[i - 1] = b;
//    }
//    BindingExpr byBinding = bindings.get(0);
//    assert byBinding.type == BindingExpr.Type.EQ;
//    exprs[n - 1] = byBinding;
//    exprs[n] = doExpr;
//    return exprs;
//  }
//
//  private static Expr[] makeExprs(
//      Var inVar, Expr input, Var byVar, Expr byExpr, Var intoVar, Expr doExpr)
//  {
//    Expr[] exprs = new Expr[3];
//    exprs[0] = new BindingExpr(BindingExpr.Type.IN, inVar, intoVar, input);
//    exprs[1] = new BindingExpr(BindingExpr.Type.EQ, byVar, null, byExpr);
//    exprs[2] = doExpr;
//    return exprs;
//  }


  /**
   * @param exprs
   */
  public GroupByExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * 
   * @param in
   * @param by
   * @param as
   * @param using
   * @param expand
   */
  public GroupByExpr(
      BindingExpr in, 
      BindingExpr by, 
      ArrayList<?> as, // as items must be Var or BindingExpr
      Expr using, 
      Expr expand)
  {
    super(addAsVars(makeExprs(in, by, using, expand), as));
  }

  public GroupByExpr(
      BindingExpr in, 
      BindingExpr by, 
      Var[] as,
      Expr using, 
      Expr expand)
  {
    super(addAsVars(makeExprs(in, by, using, expand), as));
  }

  /**
   * 
   * @param in
   * @param by
   * @param as
   * @param using
   * @param expand
   */
  public GroupByExpr(
      BindingExpr in, 
      BindingExpr by, 
      BindingExpr as, 
      Expr using, 
      Expr expand)
  {
    super(makeExprs(in, by, as, using, expand));
  }

  /**
   * 
   * @param in
   * @param by
   * @param as
   * @param using
   * @param expand
   */
  public GroupByExpr(
      BindingExpr in, 
      BindingExpr by, 
      Var as, 
      Expr using, 
      Expr expand)
  {
    super(makeExprs(in, by, new BindingExpr(BindingExpr.Type.EQ,as,null,Expr.NO_EXPRS), using, expand));
  }

//  /**
//   * exprs = (groupInBinding)+ groupByBinding doExpr
//   * 
//   * groupInByExpr is a BindingExpr, b: group b.var in b.expr[0] by b.var2 =
//   * b.expr[1]
//   * 
//   * @param bindings
//   * @param doExpr
//   */
//  public GroupByExpr(ArrayList<BindingExpr> bindings, Expr doExpr)
//  {
//    super(makeExprs(bindings, doExpr));
//  }
//
//  /**
//   * 
//   * @param input
//   * @param by
//   * @param doExpr
//   */
//  public GroupByExpr(BindingExpr input, BindingExpr by, Expr doExpr)
//  {
//    super(input, by, doExpr);
//  }
//
//  /**
//   * 
//   * @param inVar
//   * @param input
//   * @param byVar
//   * @param byExpr
//   * @param intoVar
//   * @param doExpr
//   */
//  public GroupByExpr(Var inVar, Expr input, Var byVar, Expr byExpr, Var intoVar, Expr doExpr)
//  {
//    super(makeExprs(inVar, input, byVar, byExpr, intoVar, doExpr));
//  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isNull()
   */
  @Override
  public Bool3 isNull()
  {
    return Bool3.FALSE;
  }

  /**
   * 
   */
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    if( i == 0 )
    {
      return Bool3.TRUE;
    }
    return Bool3.FALSE;
  }

  /**
   * @return
   */
  public final int numInputs()
  {
    return exprs[0].numChildren(); // == exprs[1].numChildren() == exprs.length - 4
  }

  /**
   * @param i
   * @return
   */
  public final BindingExpr inBinding()
  {
    return (BindingExpr)exprs[0];
  }

  /**
   * @return
   */
  public final BindingExpr byBinding()
  {
    return (BindingExpr)exprs[1];
  }

  /**
   * @return
   */
  public final Expr usingExpr()
  {
    return exprs[exprs.length - 2];
  }

  /**
   * @return
   */
  public final Expr collectExpr()
  {
    return exprs[exprs.length - 1];
  }

  /**
   * @return
   */
  public final Var byVar()
  {
    return byBinding().var;
  }

  /**
   * @param i
   * @return
   */
  public final Var getAsVar(int i)
  {
    return ((BindingExpr)exprs[i+2]).var;
  }

  /**
   * @param var
   * @return
   */
  public int getIntoIndex(Var var)
  {
    int n = exprs.length - 2;
    for (int i = 2 ; i < n; i++)
    {
      if( var == getAsVar(i) )
      {
        return i;
      }
    }
    return -1;
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
    int n = numInputs(); // TODO: special case n==1 to do e -> group
    BindingExpr in = inBinding();
    BindingExpr by = byBinding();
    exprText.print("\ngroup each ");
    exprText.print(in.var.name);
    exprText.print(" in ");
    String sep = "";
    for (int i = 0; i < n; i++)
    {
      exprText.print(sep);
      exprText.print("(");
      in.child(i).decompile(exprText, capturedVars);
      exprText.print(")");
      exprText.print(" by ");
      if( i == 0 ) // if( byBinding.var != Var.unused )
      {
        exprText.println(by.var.name);
        exprText.print(" = ");
      }
      exprText.print("(");
      by.child(i).decompile(exprText, capturedVars);
      exprText.print(")");
      exprText.print(" as ");
      exprText.print(getAsVar(i).name); 
      sep = ", ";
    }
    Expr using = usingExpr();
    if( using.isNull().maybeNot() )
    {
      exprText.println(" using (");
      using.decompile(exprText, capturedVars);
      exprText.println(")");
    }
    exprText.println(" expand (");
    collectExpr().decompile(exprText, capturedVars);
    exprText.println(")");

    capturedVars.remove(in.var);
    capturedVars.remove(by.var);
    for (int i = 0; i < n; i++)
    {
      capturedVars.remove(getAsVar(i));
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public Iter iter(final Context context) throws Exception
  {
    // TODO: the ItemHashtable is a real quick and dirty prototype.  We need to spill to disk, etc...
    final int n = numInputs();
    final BindingExpr in = inBinding();
    final BindingExpr by = byBinding();

    // usingExpr().eval(context); // TODO: comparator NYI
    ItemHashtable temp = new ItemHashtable(n); // TODO: add comparator support to ItemHashtable

    for (int i = 0; i < n; i++)
    {
      Item item;
      Iter iter = in.child(i).iter(context);
      while ((item = iter.next()) != null)
      {
        in.var.set(item);
        Item byItem = by.child(i).eval(context);
        temp.add(i, byItem, item);
      }
    }

    final ItemHashtable.Iterator tempIter = temp.iter();

    return new Iter() {
      Iter collectIter = Iter.empty;

      public Item next() throws Exception
      {
        while (true)
        {
          Item item = collectIter.next();
          if (item != null)
          {
            return item;
          }

          if (!tempIter.next())
          {
            return null;
          }

          by.var.set(tempIter.key());

          for (int i = 0; i < n; i++)
          {
            // TODO: any reason to NOT set the groups to null when empty? it was [].
            // context.setVar(getIntoVar(i), tempIter.values(i));
            Item group = tempIter.values(i);
            JArray arr = (JArray)group.get();
            if( arr.isEmpty() )
            {
              group = Item.NIL;
            }
            getAsVar(i).set(group);
          }

          collectIter = collectExpr().iter(context);
        }
      }
    };
  }

  
//  /**
//   * group (pipe1->$v1) by $k=e1<$v1>, (pipe2->$v2) by e2<$v2>, ... into einto<$k,$v1,$v2,...>
//   * ==>
//   * taggedMerge (pipe1->$v1) with $k=e1<$v1>, (pipe2->$v2) with $k=e2<$v2>, ... 
//   * -> group each $ by $k=$.k einto<$k, $v1=>$.v1, $v2=>$.v2, ...>
//   */
//  public void cogroupToGroup(Env env)
//  {
//    int n = numInputs();
//    Var groupInVar = env.makeVar("$");
//    Expr[] mergeInput = new Expr[n];
//    BindingExpr byBind = byBinding();
//    Var byVar = byVar();
//    Var keyVar = env.makeVar(byVar.name());
//    Expr ret = collectExpr();
//    for(int i = 0 ; i < n ; i++)
//    {
//      BindingExpr b = inBinding(i);
//      mergeInput[i] = b;
//      b.var2 = keyVar;
//      b.addChild(byBind.byExpr(i));
//      ret = ret.replaceVar(b.var, groupInVar, b.var.nameAsField());
//    }
//    Expr[] groupIn = new Expr[3];
//    groupIn[0] = new BindingExpr(BindingExpr.Type.IN, groupInVar, null, new TaggedMergeExpr(mergeInput));
//    groupIn[1] = new BindingExpr(BindingExpr.Type.EQ, byVar, null, PathFieldValue.byVarName(groupInVar, byVar));
//    groupIn[2] = collectExpr();
//    this.setChildren(groupIn);
//  }
//  
//  
//  /**
//   * <pipe> -> group each $i by e<$i> into agg<$i>
//   * ==>
//   * <pipe> -> partition each $i by $k=e<$i> |- aggregate each $i agg<$k,$i> -> map -|
//   * 
//   * group (pipe1->$v1) by $k=e1<$v1>, (pipe2->$v2) by e2<$v2>, ... into agg<$k,$v1,$v2,...>
//   * ==>
//   * taggedMerge (pipe1->$v1) with $k=e1<$v1>, (pipe2->$v2) with $k=e2<$v2>, ... 
//   * -> partition each $ by $k=$.k |- aggregate each $ agg<$k, $v1=>$.v1, $v2=>$.v2, ...> -> map -|
//   * 
//   * @param env
//   * @return
//   */
//  public Expr expand(Env env)
//  {
//    if( numInputs() > 1 )
//    {
//      cogroupToGroup(env);
//    }
//    
//    BindingExpr b = inBinding(0);
//    Expr into = collectExpr();
//    if( !(into instanceof ArrayExpr) || into.numChildren() != 1 )
//    {
//      throw new RuntimeException("ArrayExpr expected here (group by needs to go back to return semantics)");
//    }
//    into = into.child(0);
//    Var aggVar = env.makeVar("$");
//    into.replaceVar(b.var, aggVar);
//    into.detach();
//    Expr agg = AggregateExpr.make(env, aggVar, new VarExpr(b.var), into);
//    return new PartitionExpr(new Expr[]{ b, byBinding(), agg});
//  }
}

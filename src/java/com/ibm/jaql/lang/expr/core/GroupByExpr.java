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

import java.util.ArrayList;
import java.util.HashSet;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.metadata.MappingTable;
import com.ibm.jaql.lang.util.JsonHashTable;
import com.ibm.jaql.util.Bool3;
import com.ibm.jaql.util.FastPrinter;

import static com.ibm.jaql.json.type.JsonType.*;

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
    assert exprs.length == n + 5;
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
    assert exprs.length == as.length + 5;
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
      Expr options,
      Expr expand)
  {
    if( using == null )
    {
      using = new ConstExpr(null);
    }
    if( options == null )
    {
      options = new ConstExpr(null);
    }
    int n = in.numChildren();
    assert n == by.numChildren();
    Expr[] exprs = new Expr[n + 5];
    exprs[0] = in;
    exprs[1] = by;
    exprs[n+2] = using;
    exprs[n+3] = options;
    exprs[n+4] = expand;
    return exprs;
  }

  private static Expr[] makeExprs(
      BindingExpr in, 
      BindingExpr by, 
      BindingExpr as, 
      Expr using,
      Expr options,
      Expr expand)
  {
    assert 1 == in.numChildren();
    assert 1 == by.numChildren();
    if( using == null )
    {
      using = new ConstExpr(null);
    }
    if( options == null )
    {
      options = new ConstExpr(null);
    }
    return new Expr[]{ in, by, as, using, options, expand };
  }

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
      Expr options,
      Expr expand)
  {
    super(addAsVars(makeExprs(in, by, using, options, expand), as));
  }

  public GroupByExpr(
      BindingExpr in, 
      BindingExpr by, 
      Var[] as,
      Expr using, 
      Expr options,
      Expr expand)
  {
    super(addAsVars(makeExprs(in, by, using, options, expand), as));
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
      Expr options,
      Expr expand)
  {
    super(makeExprs(in, by, as, using, options, expand));
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
      Expr options, 
      Expr expand)
  {
    super(makeExprs(in, by, new BindingExpr(BindingExpr.Type.EQ,as,null,Expr.NO_EXPRS),
                    using, options, expand));
  }

  public Schema getSchema()
  {
    // the following calls update (as a side effect) the schema of the bound variables 
    inBinding().getSchema();
    byBinding().getSchema();

    // update variable schemata
    boolean hasInput = false;
    for (int i=0; i<numInputs(); i++)
    {
      Expr in = inBinding().byExpr(i);
      Schema inSchema = SchemaTransformation.restrictToArrayOrNull(in.getSchema());
      if (inSchema == null)
      {
        throw new IllegalArgumentException("array expected");
      }

      // update as-variable schemata
      Var asVar = getAsVar(i);
      if (inSchema.isEmpty(ARRAY,NULL).always())
      {
        // this indicates null input, i.e., the variable will never be set
        // e.g., schemaof(null -> group by $k=$ as $ into $);
        // TODO what do we do? I take null for the moment
        asVar.setSchema(SchemaFactory.nullSchema());
      }
      else
      {
        // input is a non-empty array
        hasInput = true;
        Schema asSchema = new ArraySchema(null, inSchema.elements());
        if (numInputs() > 1)
        {
          asSchema = SchemaTransformation.addNullability(asSchema);
        }
        asVar.setSchema(asSchema);
      }
    }
    
    if (!hasInput)
    {
      return SchemaFactory.emptyArraySchema();
    }
    Schema collectSchema = collectExpr().getSchema();
    if (collectSchema.is(ARRAY).always()) // hopefully true 
    {
      return new ArraySchema(null, collectSchema.elements());
    }
    else
    {
      // fallback
      return SchemaFactory.arraySchema();
    }
  }

  
  /**
   * Returns true if the Into clause list has side-effect, otherwise return false.
   */
  public boolean externalEffectIntoClause()
  {
	  if (collectExpr().getProperty(ExprProperty.HAS_SIDE_EFFECTS, true).never() &&
			  collectExpr().getProperty(ExprProperty.IS_NONDETERMINISTIC, true).never())
		  return false;
	  else
		  return true;
  }
  
  /**
   * Returns the mapping table of the INTO clause.
   */
  @Override
  public MappingTable getMappingTable()
  {
	  MappingTable mt = getMappingTable(byBinding().numChildren());
	  mt.addUnsafeMappingRecord();
	  return mt;
  }
    
  /**
   * Return the mapping table.
   * GroupBy does implicitly two-level mappings: one in the BY clause(s), and one in the INTO clause.
   * @param id: specifies which mapping table to return. 
   *	 --if id < byBinding().numChildren(), then return the BY clause mapping of child "id"
	*    --if id = byBinding().numChildren(), then return the INTO clause mapping.
   */
  @Override
  public MappingTable getMappingTable(int id)
  {
	  assert (id <= byBinding().numChildren());    

	  MappingTable mt = new MappingTable();
	  BindingExpr grp_by = byBinding();
	  int num_children = grp_by.numChildren();
	  
	  if (id < num_children)
	  {
		  //Return the mapping of the BY clause "by <variable> = <grouping items>" for child "id". 
		  Expr grp_key = grp_by.child(id);
		  
		  if (grp_key instanceof ConstExpr)          //There is no grouping key
			  return mt;
		  
		  MappingTable child_table = grp_key.getMappingTable();
		  VarExpr ve = new VarExpr(new Var(grp_by.var.name()));
		  if ((grp_key instanceof RecordExpr) || (grp_key instanceof ArrayExpr))
		  {
			  if (child_table.replaceVarInAfterExpr(new Var(grp_by.var.name())) == false)
				  return mt;
			  mt.addAll(child_table);
		  }
		  else   
			  mt.add(ve, grp_key, child_table.isSafeToMapAll());		  
	  }
	  else
	  {
		  //Return the mappings of the grouping key(s) in the INTO clause
		  if (collectExpr() instanceof ArrayExpr)
			  mt = (collectExpr().child(0)).getMappingTable();
		  else
			  mt = collectExpr().getMappingTable();
	  }	  
	  return mt;
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
    return exprs[0].numChildren(); // == exprs[1].numChildren() == exprs.length - 5
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
    return exprs[exprs.length - 3];
  }

  /**
   * @return
   */
  public final Expr optionsExpr()
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

  /** Replace the collect expression (during rewrites) */
  public final void setCollectExpr(Expr expr)
  {
    setChild(exprs.length - 1, expr);
  }

  /**
   * @return
   */
  public final Var byVar()
  {
    return byBinding().var;
  }

  
  /**
   * @return
   */
  public final Var inVar()
  {
    return inBinding().var;
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
    int n = exprs.length - 3;
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
  public void decompile(FastPrinter exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    int n = numInputs(); // TODO: special case n==1 to do e -> group
    BindingExpr in = inBinding();
    BindingExpr by = byBinding();
    exprText.print("\n" + kw("group") + " " + kw("each") + " ");
    exprText.print(in.var.taggedName());
    exprText.print(" " + kw("in") + " ");
    String sep = "";
    for (int i = 0; i < n; i++)
    {
      exprText.print(sep);
      exprText.print("(");
      in.child(i).decompile(exprText, capturedVars);
      exprText.print(")");
      exprText.print(" " + kw("by") + " ");
      if( i == 0 ) // if( byBinding.var != Var.unused )
      {
        exprText.println(by.var.taggedName());
        exprText.print(" = ");
      }
      exprText.print("(");
      by.child(i).decompile(exprText, capturedVars);
      exprText.print(")");
      exprText.print(" as ");
      exprText.print(getAsVar(i).taggedName()); 
      sep = ", ";
    }
    
    Expr using = usingExpr();
    if( using.getSchema().is(NULL).maybeNot() )
    {
      exprText.println(" " + kw("using") + " (");
      using.decompile(exprText, capturedVars);
      exprText.println(")");
    }

    Expr opts = optionsExpr();
    if( opts.getSchema().is(NULL).maybeNot() )
    {
      exprText.println(" " + kw("options") + " (");
      opts.decompile(exprText, capturedVars);
      exprText.println(")");
    }

    exprText.println(" " + kw("expand") + " (");
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
  public JsonIterator iter(final Context context) throws Exception
  {
    // TODO: the ItemHashtable is a real quick and dirty prototype.  We need to spill to disk, etc...
    final int n = numInputs();
    final BindingExpr in = inBinding();
    final BindingExpr by = byBinding();

    // usingExpr().eval(context); // TODO: comparator NYI
    JsonHashTable temp = new JsonHashTable(n); // TODO: add comparator support to ItemHashtable

    for (int i = 0; i < n; i++)
    {
      for (JsonValue value : in.child(i).iter(context))
      {
        in.var.setValue(value);
        JsonValue byValue = by.child(i).eval(context);
        temp.add(i, byValue, value);
      }
    }

    final JsonHashTable.Iterator tempIter = temp.iter();

    return new JsonIterator() {
      JsonIterator collectIter = JsonIterator.EMPTY;

      public boolean moveNext() throws Exception
      {
        while (true)
        {
          if (collectIter.moveNext())
          {
            currentValue = collectIter.current();
            return true;
          }

          if (!tempIter.next())
          {
            return false;
          }

          by.var.setValue(tempIter.key());

          for (int i = 0; i < n; i++)
          {
            // TODO: any reason to NOT set the groups to null when empty? it was [].
            // context.setVar(getIntoVar(i), tempIter.values(i));
            JsonValue group = tempIter.values(i);
            JsonArray arr = (JsonArray)group;
            if( arr.isEmpty() )
            {
              group = null;
            }
            getAsVar(i).setValue(group);
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

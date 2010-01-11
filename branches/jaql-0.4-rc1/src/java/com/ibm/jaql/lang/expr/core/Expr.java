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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.HashSet;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.Bool3;

/** Superclass for all JAQL expressions.
 * 
 */
public abstract class Expr
{
  public final static Expr[] NO_EXPRS        = new Expr[0];
  public final static int    UNLIMITED_EXPRS = Integer.MAX_VALUE;

  /** parent expression in which this expression is used, if any, otherwise null */
  protected Expr             parent;					 
  
  /** list of subexpressions (e.g., arguments) */
  protected Expr[]           exprs           = NO_EXPRS; 

  /**
   * @param exprs
   */
  public Expr(Expr[] exprs)
  {
    this.exprs = exprs;
    for (int i = 0; i < exprs.length; i++)
    {
      if (exprs[i] != null)
      {
        // this is not true during some rewrites
        // assert exprs[i].parent == null;
        if( exprs[i] instanceof InjectAboveExpr )
        {
          exprs[i].replaceInParent(this);
          exprs[i] = exprs[i].exprs[0];
        }
        exprs[i].parent = this;
      }
    }
  }

  /**
   * @param expr0
   */
  public Expr(Expr expr0)
  {
    this(new Expr[]{expr0});
  }

  /**
   * @param expr0
   * @param expr1
   */
  public Expr(Expr expr0, Expr expr1)
  {
    this(new Expr[]{expr0, expr1});
  }

  /**
   * @param expr0
   * @param expr1
   * @param expr2
   */
  public Expr(Expr expr0, Expr expr1, Expr expr2)
  {
    this(new Expr[]{expr0, expr1, expr2});
  }

  /**
   * @param expr0
   * @param expr1
   * @param expr2
   * @param expr3
   */
  public Expr(Expr expr0, Expr expr1, Expr expr2, Expr expr3)
  {
    this(new Expr[]{expr0, expr1, expr2, expr3});
  }

  /**
   * @param exprs
   */
  public Expr(ArrayList<? extends Expr> exprs)
  {
    this(exprs.toArray(new Expr[exprs.size()]));
  }

  /** Decompiles this expression. The resulting JSON code is written to <tt>exprText</tt>
   * stream and all variables referenced by this expression are added <tt>capturedVars</tt>.
   * 
   * <p> The default implementation only works for built-in functions that do not capture
   * variables (over the ones used in their arguments). It must be overridden for non-functions 
   * and functions that capture variables.
   * 
   * @param exprText
   * @param capturedVars
   * @throws Exception
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    JaqlFn fn = this.getClass().getAnnotation(JaqlFn.class);
    exprText.print(fn.fnName());
    exprText.print("(");
    String sep = "";
    for (Expr e : exprs)
    {
      exprText.print(sep);
      e.decompile(exprText, capturedVars);
      sep = ", ";
    }
    exprText.print(")");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString()
  {
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    try
    {
      PrintStream exprText = new PrintStream(outStream, false, JaqlUtil.getEncoding());
      HashSet<Var> capturedVars = new HashSet<Var>();
      this.decompile(exprText, capturedVars); // TODO: capturedVars should be optional
      exprText.flush();
      return outStream.toString(JaqlUtil.getEncoding());
    }
    catch (Exception e)
    {
      return "exception: " + e;
    }
  }

  /** Evaluates this expression and return the result as an Item. 
   * 
   * @param context
   * @return
   * @throws Exception
   */
  public abstract Item eval(Context context) throws Exception;

  //  public void write(Context context, TableWriter writer) throws Exception
  //  {
  //    writer.write(eval(context));
  //  }

  /**
   * Evaluates this expression. If it produces an array, this method returns an iterator
   * over the elements of the array. If it produces null, this method returns a null
   * iterator. Otherwise, it throws a cast error.
   * 
   * @param context
   * @return
   * @throws Exception
   */
  public Iter iter(Context context) throws Exception
  {
    Item item = eval(context);
    JValue w = item.get();
    if (w == null)
    {
      return Iter.nil;
    }
    JArray array = (JArray) w; // intentional cast error is possible
    return array.iter();
  }

  // TODO: kill this?
  /**
   * Notify this node and all parents that this subtree was modified.
   */
  protected void subtreeModified()
  {
    //    // Walk up the tree and update any FunctionExpr's
    //    // TODO: clean this up.  See FunctionExpr.
    //    Expr p,c;
    //    for( c = this, p = parent  ; p != null ; c = p, p = c.parent )
    //    {
    //      if( p instanceof FunctionExpr ) 
    //      {
    //        FunctionExpr f = (FunctionExpr)p;
    //        f.fn.setBody(c);
    //      }          
    //    }
  }

  /**
   * true iff this expression is compile-time provably constant. ie, not:
   * nondeterministic (eg, random number generator) side-effect producing (eg,
   * write to file, send mail) uses a variable that is not defined in this scope
   * 
   * @return
   */
  public boolean isConst() 
  {
    for (Expr e : exprs)
    {
      if (!e.isConst())
      {
        return false; // cache this?
      }
    }
    return true;
  }

  /**
   * true iff this expression is compile-time provably always null.
   * 
   * @return
   */
  public Bool3 isNull()
  {
    return Bool3.UNKNOWN;
  }

  /**
   * true if this expression is compile-time provably always an array or null
   * result. (use isNullable() to eliminate null case)
   * 
   * @return
   */
  public Bool3 isArray()
  {
    return Bool3.UNKNOWN;
  }

  /**
   * true iff this expression is compile-time provably an empty array or null
   * (use isNullable() to eliminate null case)
   * 
   * @return
   */
  public Bool3 isEmpty() // TODO: improve this
  {
    return Bool3.UNKNOWN;
  }

  /**
   * true iff this expression is compile-time provably going to be
   * evaluated once per evaluation of its parent. If the parent is 
   * evaluated multiple times, this expression might still be 
   * evaluated multiple times.
   * 
   * @return
   */
  public final Bool3 isEvaluatedOnceByParent()
  {
    return parent.evaluatesChildOnce(getChildSlot());
  }

  /**
   * true iff this expression is compile-time provably going to be
   * evaluated once per evaluation of its parent. If the parent is 
   * evaluated multiple times, this expression might still be 
   * evaluated multiple times.
   * 
   * @return
   */
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.UNKNOWN; // TODO: we could (should?) make the default be true because very few operators reval their children
  }

  /**
   * @return
   */
  public final Expr parent()
  {
    return parent;
  }

  /**
   * @return
   */
  public final int numChildren()
  {
    return exprs.length;
  }

  /**
   * @param i
   * @return
   */
  public final Expr child(int i)
  {
    return exprs[i];
  }

  /**
   * Try not to use this method, and certainly DO NOT MODIFY the array!
   * 
   * @return
   */
  public final Expr[] children()
  {
    return exprs;
  }

  /**
   * @param e
   */
  public void addChild(Expr e)
  {
    if( e instanceof InjectAboveExpr ) // TODO: this really looks like hacking
    {
      this.parent = e.parent;
      e = e.exprs[0];
    }
    e.parent = this;
    Expr[] es = new Expr[exprs.length + 1];
    System.arraycopy(exprs, 0, es, 0, exprs.length);
    es[exprs.length] = e;
    exprs = es;
    subtreeModified();
  }

  /**
   * @param e
   */
  public void addChildren(ArrayList<? extends Expr> es)
  {
    int n = exprs.length;
    int m = es.size();
    Expr[] exprs2 = new Expr[n + m];
    System.arraycopy(exprs, 0, exprs2, 0, n);
    for( int i = 0 ; i < m ; i++ )
    {
      Expr e = es.get(i);
      if( e instanceof InjectAboveExpr )
      {
        this.parent = e.parent;
        e = e.exprs[0];
      }
      e.parent = this;
      exprs2[n + i] = e;
    }
    exprs = exprs2;
    subtreeModified();
  }

  /** In the parent of this expression, replace this expression by the given expression.
   * @param replaceBy
   */
  public void replaceInParent(Expr replaceBy)
  {
    // This expr is expected to have a parent.  
    // The root expr should never have this method called. 
    Expr[] es = parent.exprs;
    // This expr is expected to be found in its parent's children.
    // This will throw an index exception if the tree is not proper.
    int i = 0;
    while (es[i] != this)
    {
      i++;
    }

    es[i] = replaceBy;
    replaceBy.parent = parent;

    subtreeModified();
    // parent = null;
  }

  /** Returns the index of this expression in the parent's list of child expressions.
   * @return
   */
  public int getChildSlot()
  {
    if (parent != null)
    {
      Expr[] es = parent.exprs;
      for (int i = 0; i < es.length; i++)
      {
        if (es[i] == this)
        {
          return i;
        }
      }
    }
    return -1;
  }

  /** Removes the child expression at index in.
   * @param i
   */
  public void removeChild(int i)
  {
    if (i >= 0)
    {
      Expr[] es = new Expr[exprs.length - 1];
      System.arraycopy(exprs, 0, es, 0, i);
      System.arraycopy(exprs, i + 1, es, i, es.length - i);
      exprs = es;
    }
    subtreeModified();
  }

  /** Detaches this expression from it's parent.
   * 
   */
  public void detach()
  {
    if (parent != null)
    {
      int i = getChildSlot();
      parent.removeChild(i);
      parent = null;
    }
  }

  /**
   * Can be used to inject a box above another box like this: Expr e =
   * expr.parent.setChild(expr.getChildSlot(), new FooExpr(expr))
   * 
   * @param i
   * @param e
   * @return
   */
  public Expr setChild(int i, Expr e)
  {
    //    if( exprs[i] != null )
    //    {
    //      exprs[i].parent = null;
    //    }
    if( e instanceof InjectAboveExpr )
    {
      this.parent = e.parent;
      e = e.exprs[0];
    }
    exprs[i] = e;
    e.parent = this;
    subtreeModified();
    return e;
  }
  
  /**
   * Replace all the children expressions.
   * 
   * @param exprs
   */
  public void setChildren(Expr[] exprs)
  {
    for( Expr e: exprs )
    {
      e.parent = this;
    }
    this.exprs = exprs;
    subtreeModified();
  }

  /**
   * Replace all VarExpr(oldVar) with VarExpr(newVar)
   * 
   * @param oldVar
   * @param newVar
   */
  public void replaceVar(Var oldVar, Var newVar)
  {
    for (int i = 0; i < exprs.length; i++)
    {
      exprs[i].replaceVar(oldVar, newVar);
    }
  }
  
  /**
   * Replace all uses of $oldVar with $recVar.fieldName
   * 
   * @param oldVar
   * @param recVar
   * @param fieldName
   * @return
   */
  public Expr replaceVar(Var oldVar, Var recVar, String fieldName)
  {
    for (int i = 0; i < exprs.length; i++)
    {
      exprs[i].replaceVar(oldVar, recVar, fieldName);
    }
    return this;
  }

  /**
   * Return the list of VarExpr's that reference the given variable in this subtree.
   * 
   * @param var
   * @param exprs
   */
  public void getVarUses(Var var, ArrayList<Expr> uses)
  {
    for( Expr e: exprs )
    {
      e.getVarUses(var, uses);
    }
  }

  /**
   * @param varMap
   * @return
   */
  public Expr clone(VarMap varMap)
  {
    Expr[] es = cloneChildren(varMap);
    try
    {
      Constructor<? extends Expr> cons = this.getClass().getConstructor(
          Expr[].class);
      return cons.newInstance(new Object[]{es});
    }
    catch (Exception e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }

  /**
   * @param varMap
   * @return
   */
  public Expr[] cloneChildren(VarMap varMap)
  {
    Expr[] es;
    if (exprs.length == 0)
    {
      es = NO_EXPRS;
    }
    else
    {
      es = new Expr[exprs.length];
      for (int i = 0; i < es.length; i++)
      {
        if (exprs[i] != null)
        {
          es[i] = exprs[i].clone(varMap);
        }
      }
    }
    return es;
  }

  /**
   * returns the distance to the ancestor. getDepth(x,x) == 0 getDepth(x,null)
   * is depth in tree (with root == 1) ancestor must exist above this expr, or a
   * null pointer exception will be raised.
   * 
   * @param ancestor
   * @return
   */
  public int getDepth(Expr ancestor)
  {
    int d = 0;
    for (Expr e = this; e != ancestor; e = e.parent)
    {
      d++;
    }
    return d;
  }

  /**
   * @return
   */
  public HashSet<Var> getCapturedVars()
  {
    // FIXME: this needs to be more efficient... Perhaps I should cache expr properties...
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(outStream);
    HashSet<Var> capturedVars = new HashSet<Var>();
    try
    {
      decompile(ps, capturedVars);
    }
    catch (Exception e)
    {
      throw new UndeclaredThrowableException(e);
    }
    return capturedVars;
  }

  public InjectAboveExpr injectAbove()
  {
    Expr p = this.parent;
    InjectAboveExpr e = new InjectAboveExpr();
    this.replaceInParent(e);
    e.addChild(this);
    e.parent = p;
    return e;
  }

  public final Expr replaceVarUses(Var var, Expr replaceBy, VarMap varMap)
  {
    if(this instanceof VarExpr)
    {
      if( ((VarExpr)this).var == var )
      {
        varMap.clear();
        return replaceBy.clone(varMap);
      }
        return this;
    }
    for( Expr e: exprs )
    {
      Expr e2 = e.replaceVarUses(var, replaceBy, varMap);
      if( e2 != e )
      {
        e.replaceInParent(e2);
      }
    }
    return this;
  }

}

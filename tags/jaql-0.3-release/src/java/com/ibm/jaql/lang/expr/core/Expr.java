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
import com.ibm.jaql.util.Bool3;

/**
 * 
 */
/**
 * 
 */
public abstract class Expr
{
  public final static Expr[] NO_EXPRS        = new Expr[0];
  public final static int    UNLIMITED_EXPRS = Integer.MAX_VALUE;

  protected Expr             parent;
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
   * @param exprs
   */
  public Expr(ArrayList<Expr> exprs)
  {
    this(exprs.toArray(new Expr[exprs.size()]));
  }

  /**
   * This must be overridden for non-functions and expressions that capture
   * variables.
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
    PrintStream exprText = new PrintStream(outStream);
    HashSet<Var> capturedVars = new HashSet<Var>();
    try
    {
      this.decompile(exprText, capturedVars);
      exprText.flush();
      return outStream.toString();
    }
    catch (Exception e)
    {
      return "exception: " + e;
    }
  }

  /**
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
   * Evaluates this expression. If it produces an array, it returns an iterator
   * over the elements of the array. If it produces null, it returns a null
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
   * false iff this expression is compile-time provably constant. ie, not:
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
        return false;
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
    e.parent = this;
    Expr[] es = new Expr[exprs.length + 1];
    System.arraycopy(exprs, 0, es, 0, exprs.length);
    es[exprs.length] = e;
    exprs = es;
    subtreeModified();
  }

  /**
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

  /**
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

  /**
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

  /**
   * 
   */
  public void detach()
  {
    if (parent != null)
    {
      int i = getChildSlot();
      parent.removeChild(i);
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
    exprs[i] = e;
    e.parent = this;
    subtreeModified();
    return e;
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
    subtreeModified();
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

}

//class RecProjectExpr extends Expr
//{
//  ProjPattern pattern;
//
//  public RecProjectExpr(ProjPattern pattern)
//  {
//    this.pattern = pattern;
//  }
//
//  public void decompile(PrintStream exprText, HashSet<Var> capturedVars) throws Exception
//  {
//    pattern.decompile(exprText, capturedVars);
//  }
//
//  public Item eval(final Context context) throws Exception
//  {
//    JRecord inrec = (JRecord)pattern.recExpr.eval(context).get();
//    if( inrec == null )
//    {
//      return Item.nil;
//    }
//    Item res = context.getTemp(this); // TODO: versions of getTemp that ensures the correct JaqlType
//    JRecord outrec;
//    if( res.get() != null && res.get() instanceof JRecord )
//    {
//      outrec = (JRecord)res.get();
//      outrec.clear();
//    }
//    else
//    {
//      outrec = new JRecord();
//      res.set(outrec);        
//    }
//    pattern.evalProj(context, inrec, outrec);
//    return res;
//  }
//}

//class SortItem
//{
//  public final static int UNORDERED = 0;
//  public final static int ASC       = 1;
//  public final static int DESC      = 2;
//  
//  Expr expr;
//  int order;
//}
//

//class EmptyExpr extends Expr
//{
//  public void decompile(PrintStream exprText, HashSet<Var> capturedVars) throws Exception
//  {
//    exprText.print("null");
//  }
//
//  public Item eval(final Context context) throws Exception
//  {
//    return Item.nil;
//  }
//}

// TODO: support CombineExpr, SumExpr as Distributive aggregates
//abstract class DistributiveAggregate extends Expr
//{
//  public DistributiveAggregate(Expr[] exprs)
//  {
//    super(exprs);
//  }
//  
//}

//@JaqlFn(fnName="combiner", minArgs=2, maxArgs=2)
//class CombinerExpr extends Expr
//{
//  // item combiner(fn combineFn, array partials)
//  public CombinerExpr(Expr[] exprs)
//  {
//    super(exprs);
//  }
//
//  public Item eval(final Context context) throws Exception
//  {
//    Item[] args = new Item[2]; // TODO: memory
//    Function combineFn = (Function)exprs[0].eval(context).getNonNull();
//    Iter iter = exprs[1].iter(context);    
//    
//    Item item = iter.next();
//    if( item == null )
//    {
//      return Item.nil;
//    }
//    
//    args[0] = new Item(); // TODO: memory
//    args[0].copy(item);
//    while( (item = iter.next()) != null )
//    {
//      args[1] = item;
//      item = combineFn.eval(context, args);
//      args[0].copy(item);
//    }
//    return args[0];
//  }
//}

/*
 * 
 * class LoadXmlExpr extends Expr { Expr expr;
 * 
 * public LoadXmlExpr(Expr expr) { this.expr = expr; }
 * 
 * public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
 * throws Exception { // TODO: use proper function exprText.print("loadXml(");
 * expr.decompile(exprText, capturedVars); exprText.print(")"); }
 * 
 * public Result eval(final Context context) throws Exception { StringItem uri =
 * (StringItem)expr.eval(context).atom1(); XmlConverter xc = new XmlConverter();
 * TableWriter writer = Util.makeTempWriter("x"); return
 * xc.convert(uri.getValue(), writer); } }
 */

//@JaqlFn(fnName="read", minArgs=1, maxArgs=1)
//class ReadExpr extends IterExpr
//{
//  public ReadExpr(Expr[] exprs)
//  {
//    super(exprs);
//  }
//
//public boolean isConst()
//{
//  return false;
//}
//
//  public Iter iter(Context context) throws Exception
//  {
//    JString filename = (JString)exprs[0].eval(context).get();
//    final FileInputStream file = new FileInputStream(filename.toString());
//    final DataInput input = new DataInputStream(new BufferedInputStream(file));
//    
//    return new Iter() 
//    {
//      Item item = new Item(); // TODO: memory
//      
//      public Item next() throws IOException
//      {
//        try
//        {
//          item.readFields(input);
//          return item;
//        }
//        catch( EOFException e )
//        {
//          file.close();
//          return null;
//        }
//      }
//    };
//  }
//}
//
//@JaqlFn(fnName="write", minArgs=2, maxArgs=2)
//class WriteExpr extends Expr
//{
//  public WriteExpr(Expr[] exprs)
//  {
//    super(exprs);
//  }
//  
//public boolean isConst()
//{
//return false;
//}
//
//  public Item eval(Context context) throws Exception
//  {
//    Item fileitem = exprs[0].eval(context);
//    JString filename = (JString)fileitem.get();
//    FileOutputStream file = new FileOutputStream(filename.toString());
//    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(file));
//    
//    Item item;
//    Expr dataExpr = exprs[1];
//    Iter iter = dataExpr.iter(context);
//    while( (item = iter.next()) != null )
//    {
//      item.write(out);
//    }
//    out.flush();
//    file.close();
//    return fileitem;
//  }
//}

//abstract class Agg
//{
//  public abstract void init();
//  public abstract void add(Item item);
//  public abstract Item result();
//}
//
//class CountAgg extends Agg
//{
//  long n;
//  public void init() { n = 0; }
//  public void add(Item item) { n++; }
//  public Item result() { return new DecimalItem(n); }
//}
//
//class AggExpr extends Expr
//{
//  Expr expr;
//  Agg[] aggs;
//
//  public AggExpr(Expr expr, Agg[] aggs)
//  {
//    this.expr = expr;
//    this.aggs = aggs;
//  }
//  
//  public void decompile(PrintStream exprText, HashSet<Var> capturedVars) throws Exception
//  {
//    throw new Exception("NYI");
//  }
//
//  public Result eval(final Context context) throws Exception
//  {
//    Iter iter = expr.eval(context).iter();
//    Item item = null;
//
//    for( int i = 0 ; i < aggs.length ; i++ )
//    {
//      aggs[i].init();
//    }
//
//    while( (item = iter.next()) != null )
//    {
//      for( int i = 0 ; i < aggs.length ; i++ )
//      {
//        aggs[i].add(item);
//      }
//    }
//
//    MemoryTable seq = new MemoryTable();
//    for( int i = 0 ; i < aggs.length ; i++ )
//    {
//      seq.add( aggs[i].result() );
//    }
//
//    return new Cell("aggs", seq);
//  }
//}
//// $a.[n] == for $x in $a return $x.n
//class ProjectValueExpr extends IterExpr
//{
//  // exprs[0].[exprs[1]]
//  public ProjectValueExpr(Expr[] exprs)
//  {
//    super(exprs);
//  }
//
//  public ProjectValueExpr(Expr array, Expr name)
//  {
//    super(new Expr[]{ array, name });
//  }
//  
//  public void decompile(PrintStream exprText, HashSet<Var> capturedVars) throws Exception
//  {
//    exprText.print("(");
//    exprs[0].decompile(exprText, capturedVars);
//    exprText.print(").[");
//    exprs[1].decompile(exprText, capturedVars);
//    exprText.print("]");
//  }
//
//  @Override
//  public Iter iter(final Context context) throws Exception
//  {
//    return new Iter()
//    {
//      Iter iter = exprs[0].iter(context);
//      JString name = (JString)exprs[1].eval(context).get();
//
//      public Item next() throws Exception
//      {
//        Item item = iter.next();
//        if( item != null )
//        {
//          JRecord rec = (JRecord)item.get();
//          if( rec == null )
//          {
//            return Item.nil;
//          }
//          return rec.getValue(name);
//        }
//        return null;
//      }
//    };
//  }
//}

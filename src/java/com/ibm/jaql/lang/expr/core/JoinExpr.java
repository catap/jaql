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
import com.ibm.jaql.json.type.SpillJArray;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.json.util.ScalarIter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.util.ItemHashtable;

/**
 * 
 */
public class JoinExpr extends IterExpr // TODO: rename to equijoin
{
  /**
   * @param bindingOns: [BindingExpr, OnExpr, BindingExpr, OnExpr, ...]
   * @param collectExpr
   * @return
   */
  private static Expr[] makeExprs(ArrayList<BindingExpr> bindings, ArrayList<Expr> ons, Expr collectExpr)
  {
    int n = bindings.size();
    assert n == ons.size();
    Expr[] exprs = new Expr[2*n + 1];
    for (int i = 0; i < n; i++)
    {
      exprs[2*i]   = bindings.get(i);
      exprs[2*i+1] = ons.get(i);
    }
    exprs[exprs.length-1] = collectExpr;
    return exprs;
  }

  /**
   * @param exprs
   */
  public JoinExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * exprs = (bindingExpr)+ returnExpr
   * 
   * @param bindings
   * @param returnExpr
   */
  public JoinExpr(ArrayList<BindingExpr> bindings, ArrayList<Expr> ons, Expr returnExpr)
  {
    super(makeExprs(bindings, ons, returnExpr));
  }

  /**
   * @return
   */
  public int numBindings()
  {
    return (exprs.length - 1)/2;
  }

  /**
   * @param i
   * @return
   */
  public BindingExpr binding(int i)
  {
    assert i < numBindings();
    return (BindingExpr) exprs[2*i];
  }

  /**
   * 
   * @param i
   * @return
   */
  public Expr onExpr(int i)
  {
    assert i < numBindings();
    return exprs[2*i+1];
  }

  /**
   * @return
   */
  public Expr collectExpr()
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
    exprText.print("\nequijoin ");
    int n = numBindings();
    String sep = "";
    for (int i = 0; i < n; i++)
    {
      exprText.print(sep);
      BindingExpr b = binding(i);
      if( b.preserve )
      {
        exprText.print("preserve ");
      }
      exprText.print(b.var.name);
      exprText.print(" in (");
      b.inExpr().decompile(exprText, capturedVars);
      exprText.print(") on (");
      onExpr(i).decompile(exprText, capturedVars);
      exprText.print(")");
      sep = ",\n     ";
    }
    exprText.print("\nexpand (");
    collectExpr().decompile(exprText, capturedVars);
    exprText.println(")");

    for (int i = 0; i < n; i++)
    {
      BindingExpr b = binding(i);
      capturedVars.remove(b.var);
    }
  }

  /**
   * Put the preserved inputs first.
   * 
   * @return The number of preserved inputs
   */
  public int putPreservedFirst()
  {
    final int n = numBindings(); 
    
    // Reorder inputs such that all preserved inputs are first
    int i;
    int numPreserved = 0;
    for (i = 0; i < n; i++)
    {
      if( binding(i).preserve )
      {
        numPreserved++;
      }
    }
    if( numPreserved < n )
    {
      int j = 1;
      for( i = 0 ; i < numPreserved ; i++ )
      {
        if( ! binding(i).preserve )
        {
          if( j <= i ) j = i + 1;
          for( ; ! binding(j).preserve ; j++ )
          {
          }
          Expr t = exprs[2*i];
          exprs[2*i] = exprs[2*j];
          exprs[2*j] = t;
          t = exprs[2*i+1];
          exprs[2*i+1] = exprs[2*j+1];
          exprs[2*j+1] = t;
          j++;
        }
      }
    }
    return numPreserved;
  }
  
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public Iter iter(final Context context) throws Exception
  {
    // TODO: the ItemHashtable is a real quick and dirty prototype.  We need to spill to disk, etc...
    final int n = numBindings();
    final int lastPreserved = putPreservedFirst() - 1; // TODO: this should be compile time.
    
    ItemHashtable temp = new ItemHashtable(n);
    final ScalarIter[] nilIters = new ScalarIter[n];

    final SpillJArray nullKeyResults = new SpillJArray();

    for (int i = 0; i < n; i++ )
    {
      context.setVar(binding(i).var, Item.nil);
    }

    for (int i = 0; i < n; i++ )
    {
      BindingExpr b = binding(i);
      Expr on = onExpr(i);
      Item item;
      Iter iter = b.inExpr().iter(context);
      while ((item = iter.next()) != null)
      {
        context.setVar(b.var, item);
        Item key = on.eval(context);
        if( ! key.isNull() )
        {
          temp.add(i, key, item);
        }
        else if( i <= lastPreserved )
        {
          context.setVar(b.var, item);
          nullKeyResults.addAll(collectExpr().iter(context));
        }
      }
      context.setVar(b.var, Item.nil);

      // If more than one is preserved, we do the outer-cross product of matching items,
      //   and filter the where at least one preserved input is non-null.
      // If exactly one is preserved, we avoid the null case on the preserved one and the filter.
      if( lastPreserved >= 0 ) 
      {
        nilIters[i] = new ScalarIter(Item.nil);
      }
    }

    final ItemHashtable.Iterator tempIter = temp.iter();
    final Iter[] groupIters = new Iter[n];

    return new Iter() {
      int  i           = -1;
      Iter collectIter = nullKeyResults.iter();
      int firstNonEmpty = n;

      public Item next() throws Exception
      {
        while( true )
        {
          Item item = collectIter.next();
          if( item != null )
          {
            return item;
          }

          do
          {
            if( i < 0 )
            {
              if( !tempIter.next() )
              {
                return null;
              }
              
              // Item key = tempIter.key();

              firstNonEmpty = n;
              for( int j = 0; j < n; j++ )
              {
                resetIter(j); 
              }

              i = 0;
            }

            BindingExpr b = binding(i);
            item = groupIters[i].next();
            if (item != null)
            {
              context.setVar(b.var, item);
              i++;
            }
            else
            {
              resetIter(i); 
              i--;
            }
          } while (i < n);

          i = n - 1;
          collectIter = collectExpr().iter(context);
        }
      }

      
      /**
       * 
       * @param j
       * @param firstNonEmpty
       * @return True iff the input is non-empty
       * @throws Exception 
       */
      private void resetIter(int j) throws Exception
      {
        Item item = tempIter.values(j);
        
        JArray arr = (JArray) item.get();
        if( !arr.isEmpty() )
        {
          groupIters[j] = arr.iter(); // TODO: should be able to reuse array iterator
          if( j < firstNonEmpty )
          {
            firstNonEmpty = j;
          }
        }
        else // arr.isEmpty()
        {
          if( lastPreserved >= 0 &&              // Some input is preserved 
              ( j != lastPreserved ||            // This input is not the last preserved input
                firstNonEmpty < lastPreserved )) // Some earlier preserved input is non-empty
          {
            nilIters[j].reset(Item.nil);
            groupIters[j] = nilIters[j];
          }
          else
          {
            groupIters[j] = Iter.empty;
          }
        }
      }
    };
  }

}

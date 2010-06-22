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

import static com.ibm.jaql.json.type.JsonType.NULL;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.json.util.SingleJsonValueIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.metadata.MappingTable;
import com.ibm.jaql.lang.util.JsonHashTable;
import com.ibm.jaql.util.Bool3;

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
  private static Expr[] makeExprs(
      ArrayList<BindingExpr> bindings,
      ArrayList<Expr> ons,
      Expr optionsExpr,
      Expr collectExpr)
  {
    if( optionsExpr == null )
    {
      optionsExpr = new ConstExpr(null);
    }
    int n = bindings.size();
    assert n == ons.size();
    Expr[] exprs = new Expr[2*n + 2];
    for (int i = 0; i < n; i++)
    {
      exprs[2*i]   = bindings.get(i);
      exprs[2*i+1] = ons.get(i);
    }
    exprs[exprs.length-2] = optionsExpr;
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
  public JoinExpr(ArrayList<BindingExpr> bindings, ArrayList<Expr> ons, Expr options, Expr returnExpr)
  {
    super(makeExprs(bindings, ons, options, returnExpr));
  }

  /**
   * @return
   */
  public int numBindings()
  {
    return (exprs.length - 2)/2;
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

  public Expr optionsExpr()
  {
    return exprs[exprs.length - 2];
  }

  /**
   * @return
   */
  public Expr collectExpr()
  {
    return exprs[exprs.length - 1];
  }

  
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    if( i < numBindings() * 2 && i % 2 == 0 ) // i is an input binding
    {
      return Bool3.TRUE;
    }
    return Bool3.UNKNOWN;
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
    exprText.print("\n" + kw("equijoin") + " ");
    int n = numBindings();
    String sep = "";
    for (int i = 0; i < n; i++)
    {
      exprText.print(sep);
      BindingExpr b = binding(i);
      if( b.preserve )
      {
        exprText.print(kw("preserve") + " ");
      }
      exprText.print(b.var.taggedName());
      exprText.print(" " + kw("in") + " (");
      b.inExpr().decompile(exprText, capturedVars);
      exprText.print(") " + kw("on") + " (");
      onExpr(i).decompile(exprText, capturedVars);
      exprText.print(")");
      sep = ",\n     ";
    }
    
    Expr opts = optionsExpr();
    if( opts.getSchema().is(NULL).maybeNot() )
    {
      exprText.println(" " + kw("options") + " (");
      opts.decompile(exprText, capturedVars);
      exprText.println(")");
    }
    
    exprText.print("\n" + kw("expand") + " (");
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
  
  
  /**
   * Return one mapping table that merges the mapping table from both children.
   */
  @Override
  public MappingTable getMappingTable()
  {
	  MappingTable mt = new MappingTable();	  
	  mt.addAll((binding(0).inExpr()).getMappingTable());
	  mt.addAll((binding(1).inExpr()).getMappingTable());
	  mt.addUnsafeMappingRecord();
	  return mt;
  }
  
  
  /**
   * Return the mapping table for child "child_id".
   */
  @Override
  public MappingTable getMappingTable(int child_id)
  {
	  assert (child_id == 0 || child_id == 1);
	  return (binding(child_id).inExpr()).getMappingTable();
  }
   
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(final Context context) throws Exception
  {
    // TODO: the ItemHashtable is a real quick and dirty prototype.  We need to spill to disk, etc...
    final int n = numBindings();
    final int lastPreserved = putPreservedFirst() - 1; // TODO: this should be compile time.
    
    JsonHashTable temp = new JsonHashTable(n);
    final SingleJsonValueIterator[] nilIters = new SingleJsonValueIterator[n];

    final SpilledJsonArray nullKeyResults = new SpilledJsonArray();

    for (int i = 0; i < n; i++ )
    {
      binding(i).var.setValue(null);
    }

    for (int i = 0; i < n; i++ )
    {
      BindingExpr b = binding(i);
      Expr on = onExpr(i);
      JsonIterator iter = b.inExpr().iter(context);
      for (JsonValue value : iter)
      {
        b.var.setValue(value);
        JsonValue key = on.eval(context);
        if( key != null  )
        {
          temp.add(i, key, value);
        }
        else if( i <= lastPreserved )
        {
          b.var.setValue(value);
          nullKeyResults.addCopyAll(collectExpr().iter(context));
        }
      }
      b.var.setValue(null);

      // If more than one is preserved, we do the outer-cross product of matching items,
      //   and filter the where at least one preserved input is non-null.
      // If exactly one is preserved, we avoid the null case on the preserved one and the filter.
      if( lastPreserved >= 0 ) 
      {
        nilIters[i] = new SingleJsonValueIterator(null);
      }
    }

    final JsonHashTable.Iterator tempIter = temp.iter();
    final JsonIterator[] groupIters = new JsonIterator[n];

    return new JsonIterator() {
      int  i           = -1;
      JsonIterator collectIter = nullKeyResults.iter();
      int firstNonEmpty = n;

      public boolean moveNext() throws Exception
      {
        while( true )
        {
          if (collectIter.moveNext()) {
            currentValue = collectIter.current();
            return true;
          }

          do
          {
            if( i < 0 )
            {
              if( !tempIter.next() )
              {
                return false;
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
            if (groupIters[i].moveNext()) {
              b.var.setValue(groupIters[i].current());
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
        JsonValue value = tempIter.values(j);
        JsonArray arr = (JsonArray) value;
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
            nilIters[j].reset(null);
            groupIters[j] = nilIters[j];
          }
          else
          {
            groupIters[j] = JsonIterator.EMPTY;
          }
        }
      }
    };
  }

}

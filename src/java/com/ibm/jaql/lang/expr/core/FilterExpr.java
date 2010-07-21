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
import java.util.HashSet;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import static com.ibm.jaql.json.type.JsonType.*;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.metadata.MappingTable;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.Bool3;


public final class FilterExpr extends IterExpr
{
  /**
   * BindingExpr inExpr, Expr predicate
   * 
   * @param exprs
   */
  public FilterExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param inBinding
   * @param predicate: one expression contains one or more conjunctive predicates.
   */
  public FilterExpr(BindingExpr inBinding, Expr predicate)
  {
    super(makeExprs(inBinding, decomposeToConjunctives(predicate)));
  }

  /**
   * @param mapVar
   * @param inExpr
   * @param predicate: one expression contains one or more conjunctive predicates.
   */
  public FilterExpr(Var mapVar, Expr inExpr, Expr predicate)
  {
    this(new BindingExpr(BindingExpr.Type.IN, mapVar, null, inExpr),
        predicate);
  }

  /**
   * @param inBinding
   * @param predicate_list: Array of conjunctive predicates
   */
  public FilterExpr(BindingExpr inBinding, ArrayList<Expr> predicate_list)
  {
	  super(makeExprs(inBinding, predicate_list));
  }

  /**
   * @param inBinding
   * @param predicate_list: Array of conjunctive predicates
   */
  public FilterExpr(BindingExpr inBinding, Expr[] predicate_list)
  {
	  super(makeExprs(inBinding, predicate_list));
  }


  /**
   * Make a single Expr[] from the filter inputs.
   */
  private static Expr[] makeExprs(BindingExpr inBinding, Expr[] preds)
  {	  
	  Expr[] list = new Expr[preds.length + 1];
	  list[0] = inBinding;
	  for (int i = 0; i < preds.length; i++)
		  list[i+1] = preds[i];
	  return list;
  }

  /**
   * Make a single Expr[] from the filter inputs.
   */
  private static Expr[] makeExprs(BindingExpr inBinding, ArrayList<Expr> preds)
  {	  
	  Expr[] list = new Expr[preds.size() + 1];
	  list[0] = inBinding;
	  for (int i = 0; i < preds.size(); i++)
		  list[i+1] = preds.get(i);
	  return list;
  }
  
  /**
   * Decompose the predicate to a list of conjunctive predicates
   */
  private static ArrayList<Expr> decomposeToConjunctives(Expr pred)
  {
	  ArrayList<Expr> e = new ArrayList<Expr>();	  
	  if (!(pred instanceof AndExpr))
		  e.add(pred);
	  else
	  {
		  e.addAll(decomposeToConjunctives(pred.exprs[0]));
		  e.addAll(decomposeToConjunctives(pred.exprs[1]));
	  }
	  return e;
  }
  
  /**
   *  Returns the composition of the conjunctive predicates as one expression. 
   */
  public Expr composePredicates()
  {
	  VarMap vm = new VarMap();
	  Expr rslt = conjunctivePred(0).clone(vm);
	  for (int i = 1; i < conjunctivePred_count(); i++)
	  {
		  Expr parent = new AndExpr(rslt, conjunctivePred(i).clone(vm));
		  rslt = parent;
	  }
	  return rslt;

  }

  /**
   * Returns the number of conjunctive predicates
   */
  public int conjunctivePred_count()
  {
	  return (this.numChildren() - 1);
  }
  
  /**
   * Returns the conjunctive predicate at index i (starting from 0)
   */  
  public Expr conjunctivePred(int i)
  {
	  assert(i < conjunctivePred_count());
	  return this.child(i + 1);
  }

  
  /**
   * Returns an array of the conjunctive predicates
   */  
  public ArrayList<Expr> conjunctivePredList()
  {
	  ArrayList<Expr> rslt = new ArrayList<Expr>();
	  for (int i = 0; i < conjunctivePred_count(); i++)
		  rslt.add(conjunctivePred(i));
	  
	  return rslt;
  }

  /**
   * Replaces the input var used in the predicates
   */
  public boolean replaceVarInPredicates(Var old_var, Var new_var)
  {
	  for (int i = 0; i < conjunctivePred_count(); i++)
		  conjunctivePred(i).replaceVar(old_var, new_var);
	  
	  return true;
  }
  
  
  /**
   * Return the mapping table.
   */
  @Override
  public MappingTable getMappingTable()
  {
	  MappingTable mt = new MappingTable();
	  mt.addAll((binding().inExpr()).getMappingTable());
	  mt.addUnsafeMappingRecord();
	  return mt;
  }
  
  /**
   * Returns true if any predicate in the Filter has side effect nor non-determinism. Otherwise returns false.  
   */
  public boolean externalEffectPredicates()
  {
	  for (int i = 0; i < conjunctivePred_count(); i++)
	  {
		  Expr pred = conjunctivePred(i);
		  boolean noExternalEffects = 
		        pred.getProperty(ExprProperty.HAS_SIDE_EFFECTS, true).never() &&
		        pred.getProperty(ExprProperty.IS_NONDETERMINISTIC, true).never();

		  if (!noExternalEffects)
			  return true;		  
	  }
	  return false;
  }	
   
  /**
   * @return
   */
  public BindingExpr binding()
  {
    return (BindingExpr) exprs[0];
  }

  /**
   * @return
   */
  public Var var()
  {
    return binding().var;
  }


  @Override
  public Schema getSchema()
  {
    // inSchema is an array of the values that are to be filtered
    Schema inSchema = binding().getSchema();  
     
    // handle null/empty input
    if (inSchema.isEmpty(ARRAY,NULL).always())
    {
      return SchemaFactory.emptyArraySchema();
    }
    
    // handle non-empty input
    Schema rest = inSchema.elements();
    return new ArraySchema(null, rest);
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
   * This expression can be applied in parallel per partition of child i.
   */
  @Override
  public boolean isMappable(int i)
  {
    return i == 0;
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
    BindingExpr b = binding();
    b.inExpr().decompile(exprText, capturedVars);
    exprText.print("\n-> " + kw("filter") + " " + kw("each") + " ");
    exprText.print(b.var.taggedName());
    exprText.print(" (");
    conjunctivePred(0).decompile(exprText, capturedVars);
    exprText.print(" )");
    for (int i = 1; i < this.conjunctivePred_count(); i++)
    {
    	exprText.print(" and ");
        exprText.print(" (");
        conjunctivePred(i).decompile(exprText, capturedVars);    	
        exprText.print(" )");
    }
    capturedVars.remove(b.var);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  
  public JsonIterator iter(final Context context) throws Exception 
  { 
    final BindingExpr inBinding = binding(); 
    final FilterExpr  filter = this; 
    final JsonIterator inIter = inBinding.iter(context); 
 
    return new JsonIterator() { 
      public boolean moveNext() throws Exception 
      { 
        while (true) 
        { 
          if (inIter.moveNext()) { 
        	  	try{
                  boolean match = true; 
                  for (int i = 0; i < filter.conjunctivePred_count(); i++) 
                  { 
                          match = match && JaqlUtil.ebv(filter.conjunctivePred(i).eval(context)) ; 
                          if (!match) 
                                  break; 
                  } 
                  if(match) 
                  { 
                          currentValue = inIter.current(); 
                          return true; 
                  } 
        	  	}catch(Throwable t) {
        	  		JsonValue v = inBinding.var.getValue(context);
        	  		JaqlUtil.getExceptionHandler().handleException(t, v);
        	  		return false;
        	  	}
          }  
          else  
          { 
            return false; 
          }           
        } 
      } 
    }; 
  } 
}

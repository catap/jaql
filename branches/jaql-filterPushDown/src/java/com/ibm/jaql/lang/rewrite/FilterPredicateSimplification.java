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
package com.ibm.jaql.lang.rewrite;


import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.FilterExpr;


/**
 * The main class for simplifying a filter conjunctive predicates.
 * Example 1: e1 -> filter (e2 and e3 and .... and false and ...) -> en  ----->  [] -> en
 * Example 2: e1 -> filter (e2 and e3 and .... and null and ...) -> en   ----->  [] -> en 
 * Example 3: e1 -> filter (e2 and e3 and .... and true and ...) -> en   ----->  e1 -> filter (e2 and e3 and .... and ...) -> en
 * Example 4: e1 -> filter (true) -> en   ----->  e1 -> en
 */
public class FilterPredicateSimplification extends Rewrite
{
  /**
   * @param phase
   */
  public FilterPredicateSimplification(RewritePhase phase)
  {
    super(phase, FilterExpr.class);
  }

  private boolean predicate_simplification(FilterExpr fe) 
  {
	  ArrayExpr empty_input = new ArrayExpr();
	  
	  //Loop over the conjunctive predicates 
	  for (int i = 0; i < fe.conjunctivePred_count(); i++)
	  {
		  Expr pred = fe.conjunctivePred(i);
		  if (pred instanceof ConstExpr)
		  {
			  JsonValue pred_val = ((ConstExpr)pred).value;
			  if (pred_val == null)
			  {
				  fe.replaceInParent(empty_input);                 //Replace filter with empty input
				  return true;
			  }
			  else if (pred_val.equals(JsonBool.FALSE))
			  {
				  fe.replaceInParent(empty_input);                 //Replace filter with empty input
				  return true;
			  }
			  else if (pred_val.equals(JsonBool.TRUE))
			  {
				  pred.detach();                                  //Remove the predicate (and the filter expr if becomes empty)
				  if  (fe.conjunctivePred_count() == 0)
					  fe.replaceInParent(fe.binding().inExpr());
				  return true;
			  }
		  }
	  }
	  return false;
  }
  
  
  /**
   * Rewrite rule for simplifying a filter conjunctive predicates. 
   */
  @Override
  public boolean rewrite(Expr expr)
  {
	  FilterExpr fe = (FilterExpr) expr;
	  return predicate_simplification(fe);
  }
}
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

import java.util.ArrayList;

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.array.MergeFn;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.FilterExpr;
import com.ibm.jaql.lang.expr.core.GroupByExpr;
import com.ibm.jaql.lang.expr.core.JoinExpr;
import com.ibm.jaql.lang.expr.core.SortExpr;
import com.ibm.jaql.lang.expr.core.TransformExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.core.BindingExpr.Type;
import com.ibm.jaql.lang.expr.metadata.ExprMapping;
import com.ibm.jaql.lang.expr.metadata.MappingTable;
import com.ibm.jaql.lang.expr.path.PathExpr;
import com.ibm.jaql.lang.walk.ExprWalker;


/**
 * The main class for pushing Filter down in the query tree. 
 */
public class FilterPushDown extends Rewrite
{
  /**
   * @param phase
   */
  public FilterPushDown(RewritePhase phase)
  {
    super(phase, FilterExpr.class);
  }

  
  /**
   * Find PathExprs and VarExprs(that are not children of PathExpr) that contain the given var
   */
  ArrayList<Expr> findVarUseInPathExprOrVarExpr(Expr expr, Var var)
  {
    ExprWalker walker = engine.walker;
    walker.reset(expr);
    ArrayList<Expr> list = new ArrayList<Expr>();
    
    while ((expr = walker.next()) != null)
    {
    	if (expr instanceof VarExpr) 
    	{
    		VarExpr ve = (VarExpr) expr;
    		if (ve.var() == var)
    		{
    			if (expr.parent() instanceof PathExpr)
    				list.add((PathExpr) expr.parent());
    			else
    				list.add((VarExpr) expr);
    		}
    	}
    }
    return list;
  }

  /**
   * Make sure the VARs under the FilterExpr are correct. The easiest and safest way  is
   * to check all Vars under the Filter predicates and if a var is not global, then replace it with the Filter var.
   * TODO: Store the global vars in a table (stored in the TopExpr) and access it here
   */  
  private boolean consistent_filter_vars(FilterExpr filter_expr, Var old_var, Var new_var) 
  {
	  BindingExpr filter_input = filter_expr.binding();
	  filter_input.var = new_var;
	  filter_input.var2 = null;
	  filter_expr.replaceVarInPredicates(old_var, new_var);
	  return true;
  } 

  /**
   * plugin a new Filter below the child expr in all input branches specified in branch_ids.
   */
  private boolean plugin_filter_below_childExpr(ArrayList<Expr> pred_list, Var filter_var, Expr child_expr, ArrayList<Integer> branch_ids)
  {
	  VarMap vm = new VarMap();
	  for (int i  = 0 ; i < branch_ids.size(); i++)
	  {
		  //Clone the predicates 
		  ArrayList<Expr> predClone_list = new ArrayList<Expr>();
		  for (int j = 0; j < pred_list.size(); j++)
			  predClone_list.add(pred_list.get(j).clone(vm));
		  
		  //Create the new filter
		  Expr branch = child_expr.child(branch_ids.get(i));
		  BindingExpr new_filter_input = new BindingExpr(Type.IN, new Var(filter_var.name()), null, branch);
		  FilterExpr new_fe = new FilterExpr(new_filter_input, predClone_list);
		  new_fe.replaceVarInPredicates(filter_var, new_filter_input.var);
		  child_expr.setChild(branch_ids.get(i), new_fe);
	  }
	  return true;
  }  

  
  /**
   * plugin a new Filter below a Transform
   */
  private boolean plugin_filter_below_transform(ArrayList<Expr> pred_list, Var filter_var, TransformExpr transform_expr)
  {
	  BindingExpr transform_input = transform_expr.binding();
	  Var tansform_var = transform_input.var;
	  Var tansform_var2 = transform_input.var2;
	  
	  FilterExpr new_fe = new FilterExpr(transform_input, pred_list);
	  BindingExpr new_transform_input = new BindingExpr(Type.IN, tansform_var, tansform_var2, new_fe);
	  transform_expr.setChild(0, new_transform_input);

	  //Make sure the VARs under the FilterExpr are correct. 
	  consistent_filter_vars(new_fe, filter_var, new Var(filter_var.name()));
	  return true;
  }  
  
  /**
   * plugin a new Filter below a Join in the given child direction
   */
  private boolean plugin_filter_below_join(ArrayList<Expr> pred_list, Var filter_var, JoinExpr join_expr, int child_id)
  {
	  BindingExpr input = join_expr.binding(child_id);
	  Expr be_input = input.inExpr();
	  int slot_num = be_input.getChildSlot(); 
	  
	  BindingExpr new_filter_input = new BindingExpr(Type.IN, new Var(filter_var.name()), null, be_input);
	  FilterExpr new_fe = new FilterExpr(new_filter_input, pred_list);
	  input.setChild(slot_num, new_fe);
	  
	  //Make sure the VARs under the FilterExpr are correct. 
	  consistent_filter_vars(new_fe, filter_var, new_filter_input.var);
	  return true;
  }  
 
  /**
   * plugin a new Filter below a GroupBy 
   */
  private boolean plugin_filter_below_groupBy(ArrayList<Expr>[] pred_list_arr, Var filter_var, GroupByExpr grp_expr) 
  {
	  BindingExpr grp_input = grp_expr.inBinding();
	  int child_cnt = grp_input.numChildren();
	  assert (child_cnt == pred_list_arr.length);
	  
	  for (int i = 0; i < child_cnt; i++)
	  {
		  BindingExpr new_filter_input = new BindingExpr(Type.IN, new Var(filter_var.name()), null, grp_input.child(i));
		  FilterExpr new_fe = new FilterExpr(new_filter_input, pred_list_arr[i]);
		  grp_input.setChild(i, new_fe);

		  //Make sure the VARs under the FilterExpr are correct. 
		  consistent_filter_vars(new_fe, filter_var, new_filter_input.var);
	  }
	  return true;
  }
  
  /**
   * Change the expressions in usedIn_list according to the mapping in mappedTo_list.
   * -- usedIn_list points to Exprs in one predicate
   * -- mappedTo_list points to Exprs in the query tree
   */
  private boolean change_predicate(ArrayList<Expr> usedIn_list, ArrayList<Expr> mappedTo_list, Var old_var, Var new_var) 
  {
	  VarMap vm = new VarMap();
	  for (int i = 0; i < usedIn_list.size(); i++)
	  {
		  Expr src_expr = usedIn_list.get(i);
		  assert((src_expr instanceof PathExpr) || (src_expr instanceof VarExpr));
		  Expr  map_to = mappedTo_list.get(i).clone(vm);
		  map_to.replaceVar(old_var, new_var);
		  src_expr.replaceInParent(map_to);
	  }
	  return true;
  }

  /**
   * If the expressions in "usedIn_list" can be mapped according to the mapping table "mt", then return the mappings. Otherwise return NULL.
   */
  private ArrayList<Expr> predMappedTo(ArrayList<Expr> usedIn_list, MappingTable mt, boolean safeMappingOnly)
  {
	  ArrayList<Expr> mapped_to_list = new ArrayList<Expr>();

	  //If we managed to do the mapping for all exprs in usedIn_list, then the predicate can be mapped and pushed down
	  for (int i = 0; i < usedIn_list.size(); i++)
	  {
		  Expr pe = usedIn_list.get(i);
		  ExprMapping mapped_to = mt.findPatternMatch(pe, safeMappingOnly);
		  if (mapped_to == null)
			  return null;
		  mapped_to_list.add(mapped_to.getBeforeExpr());
	  }
	return mapped_to_list;
  }
  
  /**
   * Try to push Filter below the Transform
   */
  private boolean filter_transform_rewrite(FilterExpr filter_expr)
  {
	  TransformExpr transform_expr = (TransformExpr) filter_expr.binding().inExpr();
	  Var transform_var = transform_expr.var();
	  Var filter_pipe_var = filter_expr.binding().var;
	  MappingTable mt = transform_expr.getMappingTable();
	  if (mt.replaceVarInAfterExpr(filter_pipe_var) == false)
		  return false;

	  //Check each conjunctive predicate
	  ArrayList<Expr> pushed_pred = new ArrayList<Expr>();
	  for (int k = 0; k < filter_expr.conjunctivePred_count(); k ++)
	  {
		  Expr crnt_pred = filter_expr.conjunctivePred(k);
		  ArrayList<Expr> usedIn_list = findVarUseInPathExprOrVarExpr(crnt_pred, filter_pipe_var);  
		  if (usedIn_list.size() == 0)
		  {
			  pushed_pred.add(crnt_pred);
			  continue;
		  }
		  
		  //check if the usage of the piped var can be mapped.
		  ArrayList<Expr> mappedTo_list = predMappedTo(usedIn_list, mt, true);		  
		  if (mappedTo_list != null)
		  {
			  change_predicate(usedIn_list, mappedTo_list, transform_var, filter_pipe_var);        //has the side effect of changing "crnt_pred"
			  pushed_pred.add(crnt_pred);
		  }
	  }
	
	  if (pushed_pred.size() == 0)
		  return false;
	  
	  //Remove the pushed predicates from the main Filter. If it becomes empty, then delete this filter
	  for (int i = 0; i < pushed_pred.size(); i++)
		  pushed_pred.get(i).detach();
	  if  (filter_expr.conjunctivePred_count() == 0)
		  filter_expr.replaceInParent(filter_expr.binding().inExpr());
  
	  plugin_filter_below_transform(pushed_pred, filter_pipe_var, transform_expr);
	  return true;
  }


  /**
   * Try to push Filter below the Join
   */
  private boolean filter_join_rewrite(FilterExpr filter_expr) 
  {
	  VarMap vm = new VarMap();
	  JoinExpr join_expr = (JoinExpr) filter_expr.binding().inExpr();
	  
	  boolean left_preserve = join_expr.binding(0).preserve;
	  boolean right_preserve = join_expr.binding(1).preserve;
	  Expr left_child = join_expr.binding(0).inExpr();
	  Expr right_child = join_expr.binding(1).inExpr();
	  MappingTable left_mt = left_child.getMappingTable();
	  MappingTable right_mt = right_child.getMappingTable();  
	  Var filter_pipe_var = filter_expr.binding().var;
	  if (left_mt.replaceVarInAfterExpr(filter_pipe_var) == false)
		  return false;
	  if (right_mt.replaceVarInAfterExpr(filter_pipe_var) == false)
		  return false;
	  
	  //Check each conjunctive predicate
	  ArrayList<Expr> left_pushed_pred = new ArrayList<Expr>();
	  ArrayList<Expr> right_pushed_pred = new ArrayList<Expr>();
	  for (int k = 0; k < filter_expr.conjunctivePred_count(); k ++)
	  {
		  Expr crnt_pred = filter_expr.conjunctivePred(k);
		  ArrayList<Expr> usedIn_list = findVarUseInPathExprOrVarExpr(crnt_pred, filter_pipe_var);  
		  if (usedIn_list.size() == 0)
		  {
			  left_pushed_pred.add(crnt_pred);
			  right_pushed_pred.add(crnt_pred.clone(vm));
			  continue;
		  }

		  //Check if the usage of the piped var can be mapped from the left child.
		  //We try pushing the Filter after the left child. So we do not care if the mapping with the child is safe or not (we are not pushing below the child)
		  ArrayList<Expr> mappedTo_list = predMappedTo(usedIn_list, left_mt, false);		  
		  if ((mappedTo_list != null) && (!right_preserve))
		  {
			  left_pushed_pred.add(crnt_pred);
			  continue;
		  }
		  
		  //Check if the usage of the piped var can be mapped from the right child.
		  //We try pushing the Filter after the right child. So we do not care if the mapping with the child is safe or not (we are not pushing below the child)
		  mappedTo_list = predMappedTo(usedIn_list, right_mt, false);		  
		  if ((mappedTo_list != null) && (!left_preserve))
		  {
			  right_pushed_pred.add(crnt_pred);
			  continue;
		  }		  
	  }
	
	  if ((left_pushed_pred.size() == 0) && (right_pushed_pred.size() == 0)) 
		  return false;

	  //Remove the pushed predicates from the main Filter. If it becomes empty, then delete this filter
	  for (int i = 0; i < left_pushed_pred.size(); i++)
		  left_pushed_pred.get(i).detach();
	  for (int i = 0; i < right_pushed_pred.size(); i++)
		  right_pushed_pred.get(i).detach();
	  if  (filter_expr.conjunctivePred_count() == 0)
		  filter_expr.replaceInParent(filter_expr.binding().inExpr());

	  if (left_pushed_pred.size() > 0) 
		  plugin_filter_below_join(left_pushed_pred, filter_pipe_var, join_expr, 0);
	  if (right_pushed_pred.size() > 0) 
		  plugin_filter_below_join(right_pushed_pred, filter_pipe_var, join_expr, 1);
	  
	  return true;
  }

  
  /**
   * Push the filter below the child expression without restrictions, i.e., including all the predicates.
   */
  private boolean filter_directPush_rewrite(FilterExpr filter_expr, ArrayList<Integer> child_ids) 
  {
	  Expr child_expr = filter_expr.binding().inExpr();
	  Var filter_pipe_var = filter_expr.binding().var;
	  
	  plugin_filter_below_childExpr(filter_expr.conjunctivePredList(), filter_pipe_var, child_expr, child_ids);
	  filter_expr.replaceInParent(child_expr);
	  return true;
  }

  
  /**
   * Try to push Filter below the GroupBy
   * A filter predicate can be pushed below the GroupBy if it does not reference any of the columns in the INTO clause except the grouping keys. 
   * If there are multiple inputs to the GroupBy, then a predicate is pushed down only if it can be pushed over all child branches.
   */
  private boolean filter_groupBy_rewrite(FilterExpr filter_expr) 
  {
	  GroupByExpr grp_expr = (GroupByExpr) filter_expr.binding().inExpr();
	  Var filter_pipe_var = filter_expr.binding().var;

	  //get the Group By mapping tables
	  int grp_childNum = grp_expr.byBinding().numChildren();
	  MappingTable into_mt = grp_expr.getMappingTable(grp_childNum);         //get the mapping table of the INTO clause
	  MappingTable[] by_mts = new MappingTable[grp_childNum];
	  for (int i = 0; i < grp_childNum; i++)
		  by_mts[i] = grp_expr.getMappingTable(i);                          //get the mapping table(s) of the BY cluase(s) 

	 if (into_mt.replaceVarInAfterExpr(filter_pipe_var) == false)
		 return false;

	  ArrayList<Expr> pushed_pred = new ArrayList<Expr>();              //Pred. that will be removed from the Filter
	  ArrayList<Expr>[] pushed_to_child = new ArrayList[grp_childNum];  //Pred. that will be pushed to each child
	  for (int i = 0; i < grp_childNum; i++)
		  pushed_to_child[i] = new ArrayList<Expr>();
	  
	  //Check each conjunctive predicate. A predicate can be pushed down only if it can be pushed in ALL child branches.
	  VarMap vm = new VarMap();
	  for (int k = 0; k < filter_expr.conjunctivePred_count(); k ++)
	  {
		  Expr crnt_pred = filter_expr.conjunctivePred(k);
		  ArrayList<Expr> usedIn_list = findVarUseInPathExprOrVarExpr(crnt_pred, filter_pipe_var);  
		  if (usedIn_list.size() == 0)
		  {
			  //Predicate will be removed from the Filter and pushed to all child branches.	
			  pushed_pred.add(crnt_pred);
			  for (int i = 0; i < grp_childNum; i++)
				  pushed_to_child[i].add(crnt_pred.clone(vm));
			  continue;
		  }
		  
		  //check if the usage of the piped var can be mapped through both the INTO clause and the BY clause.
		  ArrayList<Expr> mappedTo_list1 = predMappedTo(usedIn_list, into_mt, true);
		  if (mappedTo_list1 == null)
			  continue;

		  //check if the predicate can be pushed through all child branches.
		  ArrayList<Expr>[] mappedTo_list2 = new ArrayList[grp_childNum];
		  for (int i = 0; i < grp_childNum; i++)
			  mappedTo_list2[i] = new ArrayList<Expr>();
		  
		  boolean can_be_pushed = true;
		  for (int i = 0; i < grp_childNum; i++)
		  {
			  mappedTo_list2[i] = predMappedTo(mappedTo_list1, by_mts[i], true);
			  if (mappedTo_list2[i] == null)
				  can_be_pushed = false;			  
		  }
		  if (!can_be_pushed)
			  continue;
		  
		  //Clone the crnt_pred and push it down over each child
		  for (int i = 0; i < grp_childNum; i++)
		  {
			  Expr crnt_pred_clone = crnt_pred.clone(vm);
			  ArrayList<Expr> usedIn_list_clone = findVarUseInPathExprOrVarExpr(crnt_pred_clone, filter_pipe_var);  
			  change_predicate(usedIn_list_clone, mappedTo_list2[i], grp_expr.inVar(), filter_pipe_var);         //has the side effect of changing "crnt_pred_clone"
			  pushed_to_child[i].add(crnt_pred_clone);			  
		  }		  
		  pushed_pred.add(crnt_pred);
	  }	
	  if (pushed_pred.size() == 0)
		  return false;

	  //Remove the pushed predicates from the main Filter. If it becomes empty, then delete this filter
	  for (int i = 0; i < pushed_pred.size(); i++)
		  pushed_pred.get(i).detach();
	  if  (filter_expr.conjunctivePred_count() == 0)
		  filter_expr.replaceInParent(filter_expr.binding().inExpr());

	  plugin_filter_below_groupBy(pushed_to_child, filter_pipe_var, grp_expr);
	  return true;
  }


  /**
   * Rewrite rule for pushing the filter down in the query tree. 
   */
  @Override
  public boolean rewrite(Expr expr)
  {
	  FilterExpr fe = (FilterExpr) expr;
	  BindingExpr filter_input = fe.binding();
	  
	  if ((filter_input.type == Type.IN) && (filter_input.inExpr() instanceof TransformExpr))
		  return filter_transform_rewrite(fe);
	  else if ((filter_input.type == Type.IN) && (filter_input.inExpr() instanceof JoinExpr))
		  return filter_join_rewrite(fe);
	  else if ((filter_input.type == Type.IN) && (filter_input.inExpr() instanceof GroupByExpr))
		  return filter_groupBy_rewrite(fe);
	  else if ((filter_input.type == Type.IN) && (filter_input.inExpr() instanceof SortExpr))
	  {
		  ArrayList<Integer> child_ids = new ArrayList<Integer>();
		  child_ids.add(0);
		  return filter_directPush_rewrite(fe, child_ids);
	  }
	  else if ((filter_input.type == Type.IN) && (filter_input.inExpr() instanceof MergeFn))
	  {
		  MergeFn expr_below = (MergeFn) filter_input.inExpr();
		  ArrayList<Integer> child_ids = new ArrayList<Integer>();
		  for (int i = 0; i < expr_below.numChildren(); i++)
			  child_ids.add(i);		  
		  return filter_directPush_rewrite(fe, child_ids);
	  }
	  return false;
  }
}

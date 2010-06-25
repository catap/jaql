package com.ibm.jaql.lang.expr.metadata;

import java.util.Enumeration;
import java.util.Hashtable;

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.path.PathExpr;
import com.ibm.jaql.lang.expr.path.PathFieldValue;
import com.ibm.jaql.lang.expr.path.PathReturn;
import com.ibm.jaql.lang.expr.path.PathStep;
import com.ibm.jaql.lang.walk.ExprWalker;
import com.ibm.jaql.lang.walk.PostOrderExprWalker;

/**
 * Main class for the mapping table. The main data structure is:
 *    --mappings (Hashtable<Expr, ExprMapping>): This table maps the expression in the first argument to the expression in the second argument. 
 *        If the mapping is safe (flag stored in the second argument), then this mapping can be used 
 *        to push FilterExpr below other Exprs. Otherwise, Filter is not pushed.
 *        The reason for keeping unsafe mappings is that other operations may make use of it, e.g., "Projection pushdown".  
 */
public class MappingTable
{
	//------- Data Members
	//In the hashtable, we store:
	//	  --Expr: It is the expression that we want to map from
	//    --ExprMapping: An entry specifies the map-to expression, and whether or not it is safe to do the mapping for "FilterPushdown" 
	private Hashtable<Expr, ExprMapping> mappings;
	public static final String DEFAULT_PIPE_VAR = "$";
	//-------------------
	
	public MappingTable()
	{
		mappings = new Hashtable<Expr, ExprMapping>();
	}
	
	private String getHashKey(Expr e)
	{
		return e.toString();
	}

	public void add(Expr after, Expr before, boolean safetyFlag)
	{
		ExprMapping mr = new ExprMapping(before, safetyFlag);
		mappings.put(after, mr);
	}
	
	public void addAll(MappingTable table)
	{
		mappings.putAll(table.mappings);
	}	
	
	public void clear()
	{
		mappings.clear();
	}

	/**
	 * Returns true only if it is safe to map all the entries of this mapping table.
	 */
	public boolean isSafeToMapAll()
	{
		if (mappings.isEmpty())
			return false;
		
		Enumeration<ExprMapping> e = mappings.elements();
		while (e.hasMoreElements())
		{
			if (e.nextElement().isSafeToMap() == false)
				return false;
		}
		return true;
	}

	/**
	 * Returns true only if it is safe to map any of the entries in this mapping table.
	 */
	public boolean isSafeToMapAny()
	{
		if (mappings.isEmpty())
			return false;

		Enumeration<ExprMapping> e = mappings.elements();
		while (e.hasMoreElements())
		{
			if (e.nextElement().isSafeToMap())
				return true;
		}
		return false;
	}

	/**
	 * @return an enumerator of the mapping table keys, i.e., map-from expressions.
	 */
	public Enumeration<Expr> KeyEnum()
	{
		return mappings.keys();
	}

	/**
	 * Returns the mapping expression of the given "afterExpr"
	 */
	public Expr BeforeExpr(Expr afterExpr)
	{
		ExprMapping r = mappings.get(afterExpr);
		if (r == null)
			return null;
		else
			return r.getBeforeExpr();
	}

	/**
	 * Is it safe to use the mapping of the 'afterExpr' in "FilterPushDown"
	 */
	public boolean isSafeToMapExpr(Expr afterExpr)
	{
		ExprMapping r = mappings.get(afterExpr);
		if (r == null)
			return false;
		else
			return r.isSafeToMap();
	}

	/**
	 * Returns the mapping entry (Expression & SafetyFlag) of the given "afterExpr"
	 */
	public ExprMapping mappedTo(Expr afterExpr)
	{
		ExprMapping r = mappings.get(afterExpr);
		return r;
	}
	
	
	/**
	 * Replaces the Var used in this expression with 'replaceBy' var
	 */
	public boolean replaceVarInAfterExpr(Var replaceBy) 
	{
		MappingTable mt = new MappingTable();

		Enumeration<Expr> e = this.mappings.keys();
		while (e.hasMoreElements())
		{
			Expr key = e.nextElement();
			ExprMapping r = mappings.get(key);
			VarExpr ve = findVarInExpr(key);
			if (ve == null)
				return false;
			
			key.replaceVar(ve.var(), replaceBy);
			mt.add(key, r.getBeforeExpr(), r.isSafeToMap());
		}		
		this.mappings = mt.mappings;
		return true;
	}

	/**
	 * Add a mapping record with 'isSafeToMap' flag unset. This records ensures that function isSafeToMapAll() return false.
	 */
	public void addUnsafeMappingRecord()
	{
		ConstExpr ce = new ConstExpr(new JsonString("_UnSafeToMap_"));
		PathFieldValue pfv = new PathFieldValue(ce);
		VarExpr ve = new VarExpr(new Var(MappingTable.DEFAULT_PIPE_VAR));
		PathExpr pe = new PathExpr(ve, pfv);
		
		if (QuickExactMatch(pe) == null)
		{
			ExprMapping mr = new ExprMapping(ce, false);
			mappings.put(pe, mr);
		}
	}

	/**
	 * Find the mapping expression to the given 'pe' expression. If SafeOnly is true, then we search only 
	 * for expression that is safe to map to. 
	 */
	public ExprMapping findPatternMatch(Expr pe, boolean safeOnly) 
	{
		//Lets try fast search where pe matches exactly what we have in the mapping table
		ExprMapping mapped_to = QuickExactMatch(pe);
		if (mapped_to != null)
		{
			if (safeOnly && !mapped_to.isSafeToMap())
				return null;
			else
				return mapped_to;
		}

		//Lets try fast partial match (the prefix of 'pe' matches what we have in the mapping table
		mapped_to = QuickPartialMatch(pe);
		if (mapped_to != null)
		{
			if (safeOnly && !mapped_to.isSafeToMap())
				return null;
			else
				return mapped_to;
		}
		
		//TODO: More complex searching
		return null;
	}
	
	/**
	 * Find the var used inside this expression. It has to have one, otherwise it returns null.
	 */
	protected VarExpr findVarInExpr(Expr expr)
	{
		ExprWalker walker = new PostOrderExprWalker();;
		walker.reset(expr);
		int count = 0;
		VarExpr ve = new VarExpr(new Var(""));
		
		while ((expr = walker.next()) != null)
	    {
	    	if (expr instanceof VarExpr)
	    	{
	    		count++;
	    		ve = (VarExpr) expr;
	    	}
	    }
		if (count != 1)
			return (VarExpr)null;
		else
			return ve;
	}

	/**
	 * Try to generate the difference between the super_expr and sub_expr. super_expr is prefixed with the sub_expr.
	 */
	protected Expr diffExpr(Expr super_expr, Expr sub_expr)
	{
		VarMap vm = new VarMap();
		
		if ((!(super_expr instanceof PathExpr)) || 
				(!((sub_expr instanceof PathExpr) || (sub_expr instanceof VarExpr))))
			return null;
		
		PathExpr super_pe = (PathExpr) super_expr;

		//The case where the sub_expr is a VarExpr	
		if (sub_expr instanceof VarExpr)
		{
			if (!(super_pe.input() instanceof VarExpr))    
				return null;
			else
				return super_pe.firstStep().clone(vm);
		}
		
		//The case where the sub_expr is a PathExpr (find how many steps you take to reach a PathReturn in the sub_expr, and move the same steps in super_expr)
		PathExpr sub_pe = (PathExpr) sub_expr;
		if ((!(super_pe.input() instanceof VarExpr)) || (!(sub_pe.input() instanceof VarExpr)))    
			return null;
		
		PathStep ps_sub = sub_pe.firstStep();
		PathStep ps_super = super_pe.firstStep();
		while (!(ps_sub instanceof PathReturn))
		{
			ps_sub = ps_sub.nextStep();
			ps_super = ps_super.nextStep();
			assert (!(ps_super instanceof PathReturn));
		}
		return ps_super.clone(vm);
	}
		
	/**
	 * try attaching the expr_diff to the end of before_expr.  
	 */
	protected Expr attachDiffExpr(Expr before_expr, Expr expr_diff) 
	{
		if (!(expr_diff instanceof PathStep))
			return null;
		
		if (before_expr instanceof PathExpr)
		{
			PathExpr pe_before = (PathExpr) before_expr;
			PathStep ps = pe_before.firstStep();
			while (!(ps instanceof PathReturn))
				ps = ps.nextStep();
			
			ps.replaceInParent(expr_diff);			
			VarExpr ve = findVarInExpr(before_expr);
			if (ve == null)
				return null;
			else
				return before_expr;
		}
		else if (before_expr instanceof VarExpr)
		{
			PathExpr pe = new PathExpr(before_expr, (PathStep)expr_diff);
			return pe;
		}
		else
			return null;
	}

	
	/**
	 * Search for an expression with the exact same structure
	 */
	protected ExprMapping QuickExactMatch(Expr pe) 
	{
		String pe_hashkey = getHashKey(pe);

		Enumeration<Expr> e = mappings.keys();
		while (e.hasMoreElements())
		{
			Expr after_expr = e.nextElement();
			String after_expr_hashkey = getHashKey(after_expr);
			if (after_expr_hashkey.equals(pe_hashkey))
			{
				return mappings.get(after_expr);
			}
		}
		return null;
	}
	
	/**
	 * Search for an expression that matches the prefix of pe.
	 * The match with the prefix has to exact, e.g.,:
	 * 		$.x will match with the prefix of $.x.y
	 * 		$.x will not match with the prefix of ($.x).y
	 * If match is found, then we need to take care of the remaining part in pe (part the did not match)
	 */
	protected ExprMapping QuickPartialMatch(Expr pe) 
	{
		if (!(pe instanceof PathExpr))
			return null;
		
		String pe_hashkey = getHashKey(pe);
		int  longest_match = 0;
		Expr   match_expr = new ConstExpr(0);
		
		Enumeration<Expr> e = mappings.keys();
		while (e.hasMoreElements())
		{
			Expr after_expr = e.nextElement();
			String after_expr_hashkey = getHashKey(after_expr);
			if (after_expr instanceof VarExpr)
				after_expr_hashkey = "(" + after_expr_hashkey + ")";

			if (pe_hashkey.startsWith(after_expr_hashkey) && (after_expr_hashkey.length() > longest_match))
			{
				longest_match = after_expr_hashkey.length();
				match_expr = after_expr; 
			}
		}
		if (longest_match == 0)
			return null;
		
		//Now we need to take care of the part that does not match
		ExprMapping em = mappedTo(match_expr);
		if (!(em.isSafeToMap()) || !((em.getBeforeExpr() instanceof PathExpr) || (em.getBeforeExpr() instanceof VarExpr))) 
			return null;

		Expr expr_diff = diffExpr(pe, match_expr);
		if (expr_diff == null)
			return null;
		
		VarMap vm = new VarMap();
		Expr before_expr = em.getBeforeExpr().clone(vm);
		Expr complt_expr = attachDiffExpr(before_expr, expr_diff);
		if (complt_expr == null)
			return null;
				
		ExprMapping output = new ExprMapping(complt_expr, true);
		return output;
	}
}

package com.ibm.jaql.lang.expr.metadata;

import com.ibm.jaql.lang.expr.core.Expr;

/**
 * This class define objects to which an arbitrary expression maps to. 
 */
public class ExprMapping
{
	private Expr beforeExpr;
	private boolean isSafeToMap;
	
	public ExprMapping(Expr e, boolean safetyFlag)
	{
		beforeExpr = e;
		isSafeToMap = safetyFlag;
	}
	
	public boolean isSafeToMap()
	{
		return isSafeToMap;
	}

	public Expr getBeforeExpr()
	{
		return beforeExpr;
	}
}
package com.ibm.jaql.lang.expr.core;


public abstract class ExprFromExprOrigin extends ExprOrigin {

	protected Expr originExpr;

	public ExprFromExprOrigin(Expr origin) {
		this.originExpr=origin;
	}

	public String toString() {
		return originExpr.getOrigin().toString();
	}

	public String toJaqlLocator() {
		if(originExpr.getOrigin()!=null)				
		  return originExpr.getOrigin().toJaqlLocator();
		else
	      return "#('<unknown>',0,0)";		
	}

}
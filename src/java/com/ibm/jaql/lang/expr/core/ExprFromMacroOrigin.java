package com.ibm.jaql.lang.expr.core;

public class ExprFromMacroOrigin extends ExprFromExprOrigin {

	public ExprFromMacroOrigin(Expr macroexpr) {
		super(macroexpr);
	}

	@Override
	public String getTrace()
	{
		return originExpr.getOrigin().getTrace() + " \n  expanding " + originExpr.getClass().toString();
	}
}

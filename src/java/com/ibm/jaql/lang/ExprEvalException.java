package com.ibm.jaql.lang;

import com.ibm.jaql.lang.expr.core.Expr;

/**
 * This class represents errors raised during runtime evaluation of a jaql expression.
 * The exceptions contain a reference to the expression that failed to evaluate,
 * to improve reporting.
 * @author cc
 *
 */
public class ExprEvalException extends JaqlRuntimeException {
    protected Expr expr;
    
	public ExprEvalException(Expr e, String arg0) {
		super(arg0);
		expr = e;
	}

	public ExprEvalException(Expr e,Throwable arg0) {
		super(arg0); 
		expr = e;
	}

	public ExprEvalException(Expr e, String arg0, Throwable arg1) {
		super(arg0, arg1);
		expr = e;
	}
	
	public String toString() {
	  String result=super.toString();
	  if(expr.getOrigin()!=null)
		  result+=", originating expression ends at "+expr.getOrigin().toString();
	  else
		  result+=" <unknown origin>";
	  return result; 
	}
}

package com.ibm.jaql.lang.rewrite;

import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprFromExprOrigin;

public class ExprFromRewriteOrigin extends ExprFromExprOrigin {
		protected Rewrite rewrite;
		
		ExprFromRewriteOrigin(Expr e, Rewrite rw)
		{
			super(e);
			rewrite=rw;
		}
		
		public String getTrace()
		{
			return originExpr.getOrigin().getTrace() + " \n  via " + rewrite.getClass().toString();
		}
		
}

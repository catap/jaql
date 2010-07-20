package com.ibm.jaql.benchmark.lang;

import java.io.Reader;

import com.ibm.jaql.lang.Jaql;
import com.ibm.jaql.lang.expr.core.Expr;

/* Only runs the first expression, other are discarded */
public class JaqlPrecompile extends Jaql {
	Expr e;
	
	public JaqlPrecompile(String filename, Reader in) {
		super(filename, in);
	}
	
	public void precompile() throws Exception {
		e = super.prepareNext();
	}
	
	@Override
	public Expr prepareNext() throws Exception {
		Expr next = e;
		if(e != null) {
			e = null;
		}
		
		return next;
	}
}

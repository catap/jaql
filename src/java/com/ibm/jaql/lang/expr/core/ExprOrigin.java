package com.ibm.jaql.lang.expr.core;

public abstract class ExprOrigin {
	public abstract String getTrace();
	public abstract String toJaqlLocator();
}

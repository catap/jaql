package com.ibm.jaql.lang.expr.core;

public class ExprFromSourceOrigin extends ExprOrigin {
	protected String filename;
	protected long line;
	protected long column;

	public ExprFromSourceOrigin(String f, long l, long c) {
		filename=f;
		line=l;
		column=c;
	}
	
	public ExprFromSourceOrigin(String f) {
		filename=f;
		line=0;
		column=0;
	}
	
	public String toString() { 
		return filename+":"+(line>0 ? Long.toString(line) : "unknown")
		               +":"+(line>0 ? Long.toString(column) :"unknown");
	}
	
	public String getTrace() {
	    return toString();
	}
	
	public String toJaqlLocator() {
	    return "#('"+filename+"',"+(line>0 ? Long.toString(line) : "0")
                   +","+(line>0 ? Long.toString(column) :"0")
               +")";
	}
}
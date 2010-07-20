package com.ibm.jaql.benchmark;

public interface JavaAggregate {
	public void accumulate(Object value) throws Exception;
	public Object getFinal() throws Exception;
}

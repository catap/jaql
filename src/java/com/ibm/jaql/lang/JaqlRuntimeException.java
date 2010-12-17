package com.ibm.jaql.lang;


public class JaqlRuntimeException extends RuntimeException {

	public JaqlRuntimeException(String arg0) {
		super(arg0);
	}

	public JaqlRuntimeException(Throwable arg0) {
		super(arg0);
	}

	public JaqlRuntimeException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

}

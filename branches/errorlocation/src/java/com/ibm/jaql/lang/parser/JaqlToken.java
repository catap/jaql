package com.ibm.jaql.lang.parser;

import antlr.CommonToken;

public class JaqlToken extends CommonToken {
    protected String filename;

	/**
	 * @return the filename
	 */
	public String getFilename() {
		return filename;
	}

	/**
	 * @param filename the filename to set
	 */
	public void setFilename(String filename) {
		this.filename = filename;
	}
    
}

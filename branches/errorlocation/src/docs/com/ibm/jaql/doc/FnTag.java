package com.ibm.jaql.doc;

/**
 * Abstract tag class that provides basic methods for tags.
 *
 */
public abstract class FnTag {
	protected String text;

	abstract void setTagData(String text);

	public String getText() {
		return text;
	}
}

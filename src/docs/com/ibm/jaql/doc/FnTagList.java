package com.ibm.jaql.doc;

import java.util.ArrayList;

/**
 * A container for udf tags. Additionally to a normal list it can also directly convert a tag comment to
 * a new tag instance.
 * 
 * @param <T>
 */
@SuppressWarnings("serial")
public class FnTagList<T extends FnTag> extends ArrayList<T> {
	private Class<T> typeClass;

	public FnTagList(Class<T> type) {
		typeClass = type;
	}

	public void add(String text) {
		T tagInstance;
		try {
			tagInstance = typeClass.newInstance();
			tagInstance.setTagData(text);
			add(tagInstance);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

package com.ibm.jaql.json.util;

import java.util.HashMap;

import com.ibm.jaql.json.type.JsonString;

public class FieldNameCache {
	private static final HashMap<JsonString, JsonString> cache = new HashMap<JsonString, JsonString>();
	
	/*
	 * Returns the cached field name. If the name is not already cached
	 * a immutable copy of the value is stored in the cache and returned.
	 */
	public static JsonString get(JsonString s) {
		if(cache.containsKey(s)) {
			return cache.get(s);
		} else {
			cache.put(s.getImmutableCopy(), s);
			return s;
		}
	}
}

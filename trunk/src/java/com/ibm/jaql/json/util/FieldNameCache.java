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
	  JsonString c = cache.get(s);
	  if( c != null )
	  {
	    return c;
	  }
	  c = s.getImmutableCopy();
	  cache.put(c, c);
	  return c;
	}
}

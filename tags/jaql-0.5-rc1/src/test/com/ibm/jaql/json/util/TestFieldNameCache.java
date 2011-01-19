package com.ibm.jaql.json.util;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ibm.jaql.json.type.JsonString;

public class TestFieldNameCache {

	private final String[] names = new String[] { "Herbert", "Robert", "Julia", 
		"Klaus", "Thomas", "Janine", "Anna", "Michelle", "Dirk", "Heidi", "Max" };
	
	@Test
	/*
	 * Tests wether the returned value is always equal to the inserted
	 * value
	 */
	public void testCacheReturn() throws Exception {
		for (int i = 0; i < names.length; i++) {
			JsonString field = new JsonString(names[i]);
			JsonString cached = FieldNameCache.get(field);
			assertEquals(field, cached);
		}
	}
	
	@Test
	/*
	 * Tests wether alway the identical object is returned from the cache
	 * for equal values
	 */
	public void testCache() throws Exception {
		for (int i = 0; i < names.length; i++) {
			JsonString fieldA = new JsonString(names[i]);
			JsonString fieldB = new JsonString(names[i]);
			JsonString cachedA = FieldNameCache.get(fieldA);
			JsonString cachedB = FieldNameCache.get(fieldB);
			assertTrue(cachedA == cachedB);
		}
	}
}

package com.ibm.jaql.json.type;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestString {
	private final String[] names = new String[] { "Herbert", "Robert", "Julia", 
			"Klaus", "Thomas", "Janine", "Anna", "Michelle", "Dirk", "Heidi", "Max" };

	private final JsonString empty = new JsonString("");
	@Test
	/* 
	 * Tests wether the equals function is working
	 */
	public void testEquals() throws Exception{
		for (int i = 0; i < names.length; i++) {
			JsonString nameA = new JsonString(names[i]);
			JsonString nameB = new JsonString(names[i]);
			JsonString nameOtherA = new JsonString(names[(i+1)%names.length]);
			JsonString nameOtherB = new JsonString(names[(i+1)%names.length]);
			assertTrue(nameA.equals(nameB));
			assertFalse(nameA.equals(empty));
			assertFalse(nameA.equals(nameOtherA));
			assertFalse(nameA.equals(nameOtherB));
		}
	}
}

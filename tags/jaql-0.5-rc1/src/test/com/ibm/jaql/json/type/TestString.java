/*
 * Copyright (C) IBM Corp. 2008.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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

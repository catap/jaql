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

import org.junit.Assert;
import org.junit.Test;

import com.ibm.jaql.json.parser.JsonParser;

public class TestRecord {
	
	String rec = "{ one: 1, two: 2, three: 3, four: 4, five: 5, six: 6, seven: 7}";
	@Test
	public void testRemove() throws Exception {
		JsonParser parser = new JsonParser();
		JsonRecord val = (JsonRecord)parser.parse(rec);
		
		// test 1
		BufferedJsonRecord r = new BufferedJsonRecord();
		r.setCopy(val);
		
		r.remove(3);
		Assert.assertEquals(6, r.size());
		
		r.remove(new JsonString("four")); // this one was already removed....
		Assert.assertEquals(6, r.size());
		
		r.remove(new JsonString("six"));
		Assert.assertEquals(5, r.size());
		
		r.remove(new JsonString("one"));
		Assert.assertEquals(4, r.size());
		
		r.remove(new JsonString("seven"));
		Assert.assertEquals(3, r.size());
		
		// test 2
		r = new BufferedJsonRecord();
		r.setCopy(val);
		r.sort();
		
		r.remove(3);
		Assert.assertEquals(6, r.size());
		
		r.remove(new JsonString("seven")); // this was in position 3 when sorting alphabetically
		Assert.assertEquals(6, r.size());
		
		r.remove(new JsonString("six"));
		Assert.assertEquals(5, r.size());
		
		r.remove(new JsonString("one"));
		Assert.assertEquals(4, r.size());
		
		r.remove(new JsonString("four"));
		Assert.assertEquals(3, r.size());
	}
}
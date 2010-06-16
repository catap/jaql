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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestSpilledJsonArray {

	@Test
	public void testNoSpillWithDefaultCacheSize() throws Exception {
		// generate data
		JsonValue[] iVals 		= generateLongsUpTo(SpilledJsonArray.DEFAULT_CACHE_SIZE - 50);
		
		// add to array
		SpilledJsonArray arr 	= putValues(iVals, new SpilledJsonArray());
		
		// get from array
		JsonValue[] oValsIdx	= getValuesByIdx(arr);
		JsonValue[] oValsIter	= getValuesByIter(arr);
		
		// look for diffs
		ArrayList<JsonValue[]> diffIdx 	= findDiff(iVals, oValsIdx);
		ArrayList<JsonValue[]> diffIter 	= findDiff(iVals, oValsIter);
		
		String errors = null;
		if(!diffIdx.isEmpty()) {
			errors = "IDX: " + printDiff(diffIdx);
		}
		if(!diffIter.isEmpty()) {
			if(errors == null)
				errors = printDiff(diffIter);
			else
				errors += "ITER: " + printDiff(diffIter);
		}
		assertEquals(errors, null, errors);
	}
	
	@Test
	public void testNoSpillWithOverrideCacheSize() throws Exception {
		// generate data
		JsonValue[] iVals 		= generateLongsUpTo(SpilledJsonArray.DEFAULT_CACHE_SIZE + 50);
		
		// add to array
		SpilledJsonArray arr 	= putValues(iVals, new SpilledJsonArray(SpilledJsonArray.DEFAULT_CACHE_SIZE + 100));
		
		// get from array
		JsonValue[] oValsIdx	= getValuesByIdx(arr);
		JsonValue[] oValsIter	= getValuesByIter(arr);
		
		// look for diffs
		ArrayList<JsonValue[]> diffIdx 	= findDiff(iVals, oValsIdx);
		ArrayList<JsonValue[]> diffIter 	= findDiff(iVals, oValsIter);
		
		String errors = null;
		if(!diffIdx.isEmpty()) {
			errors = "IDX: " + printDiff(diffIdx);
		}
		if(!diffIter.isEmpty()) {
			if(errors == null)
				errors = printDiff(diffIter);
			else
				errors += "ITER: " + printDiff(diffIter);
		}
		assertEquals(errors, null, errors);
	}
	
	@Test
	public void testSpillWithDefaultCacheSize() throws Exception {
		// generate data
		JsonValue[] iVals 		= generateLongsUpTo(SpilledJsonArray.DEFAULT_CACHE_SIZE + 50);
		
		// add to array
		SpilledJsonArray arr 	= putValues(iVals, new SpilledJsonArray());
		
		// get from array
		JsonValue[] oValsIdx	= getValuesByIdx(arr);
		JsonValue[] oValsIter	= getValuesByIter(arr);
		
		// look for diffs
		ArrayList<JsonValue[]> diffIdx 	= findDiff(iVals, oValsIdx);
		ArrayList<JsonValue[]> diffIter 	= findDiff(iVals, oValsIter);
		
		String errors = null;
		if(!diffIdx.isEmpty()) {
			errors = "IDX: " + printDiff(diffIdx);
		}
		if(!diffIter.isEmpty()) {
			if(errors == null)
				errors = printDiff(diffIter);
			else
				errors += "ITER: " + printDiff(diffIter);
		}
		assertEquals(errors, null, errors);
	}
	
	@Test
	public void testSpillWithOverrideCacheSize() throws Exception {
		// generate data
		JsonValue[] iVals 		= generateLongsUpTo(4);
		
		// add to array
		SpilledJsonArray arr 	= putValues(iVals, new SpilledJsonArray(2));
		
		// get from array
		JsonValue[] oValsIdx	= getValuesByIdx(arr);
		JsonValue[] oValsIter	= getValuesByIter(arr);
		
		// look for diffs
		ArrayList<JsonValue[]> diffIdx 	= findDiff(iVals, oValsIdx);
		ArrayList<JsonValue[]> diffIter 	= findDiff(iVals, oValsIter);
		
		String errors = null;
		if(!diffIdx.isEmpty()) {
			errors = "IDX: " + printDiff(diffIdx);
		}
		if(!diffIter.isEmpty()) {
			if(errors == null)
				errors = printDiff(diffIter);
			else
				errors += "ITER: " + printDiff(diffIter);
		}
		assertEquals(errors, null, errors);
	}
	
	private JsonValue[] generateLongsUpTo(int n) {
		JsonValue[] vals = new JsonValue[n];
		for(int i = 0; i < n; i++) {
			vals[i] = new JsonLong(i);
		}
		return vals;
	}
	
	private SpilledJsonArray putValues(JsonValue[] vals, SpilledJsonArray tgt) throws Exception {
		int n = vals.length;
		for(int i = 0; i < n; i++) {
			tgt.add(vals[i]);
		}
		tgt.freeze();
		return tgt;
	}
	
	private JsonValue[] getValuesByIdx(SpilledJsonArray arr) throws Exception {
		long n = arr.count();
		JsonValue[] vals = new JsonValue[(int)n];
		for(int i = 0; i < n; i++) {
			JsonValue v = arr.get(i);
			if(v != null)
				vals[i] = v.getImmutableCopy();
			else
				vals[i] = null;
		}
		return vals;
	}
	
	private JsonValue[] getValuesByIter(SpilledJsonArray arr) throws Exception {
		long n = arr.count();
		JsonValue[] vals = new JsonValue[(int)n];
		int idx = 0;
		for(JsonValue val : arr) {
			if(val != null)
				vals[idx++] = val.getImmutableCopy();
			else
				vals[idx++] = null;
		}
		return vals;
	}
	
	private ArrayList<JsonValue[]> findDiff(JsonValue[] in, JsonValue[] out) {
		ArrayList<JsonValue[]> diffs = new ArrayList<JsonValue[]>();
		if(in == null)
			assertEquals("if <in> is null, out should be null as well", out, null);
		if(out == null)
			assertEquals("if <out> is null, in should be null as well", in, null);
		if(in != null && out != null) {
			assertEquals(in.length, out.length);
			int n = in.length;
			for(int i = 0; i < n; i++) {
				JsonValue iVal = in[i];
				JsonValue oVal = out[i];
				if( (iVal != null && oVal == null) ||
					(iVal == null && iVal != null) ||
					(!iVal.equals(oVal)) ) {
					
					diffs.add(new JsonValue[] {new JsonLong(i), iVal, oVal});
				}
			}
		}
		return diffs;
	}
	
	private String printDiff(ArrayList<JsonValue[]> diff) throws Exception {
		StringBuilder sb = new StringBuilder();
		
		for(JsonValue[] e : diff) {
			sb.append("[").append(e[0]).append(":");
			sb.append(JsonUtil.printToString(e[1])).append(",");
			sb.append(JsonUtil.printToString(e[2]));
			sb.append("]");
		}
		return sb.toString();
	}
}
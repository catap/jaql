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

package com.ibm.jaql.json.type.lazy;

import java.util.Iterator;
import java.util.Map.Entry;

import com.ibm.jaql.io.serialization.binary.perf.lazy.LazyJsonInputBuffer;
import com.ibm.jaql.io.serialization.binary.perf.lazy.LazyRecordSerializer;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonEncoding;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;


/** An in-memory {@link JsonRecord}. Name-based field access is implemented using a hash table.
 * In addition, this class provides functionality to access record fields by index. This is 
 * possible because the main data structure used by this record is a parallel array: one array 
 * storing the field names and one array storing the corresponding field values. As long as the 
 * record names are not modified, field indexes are stable and can be used to avoid the look-up 
 * cost of name-based accesses.
 * 
 * Quick record construction is supported via the various <code>setInternal</code> methods.  
 */
public class LazyJsonRecord extends JsonRecord {
	private final Schema schema;
	private final LazyRecordSerializer serializer;
	private JsonRecord decodedValue;
	private LazyJsonInputBuffer buffer;
	private boolean decoded;

	// -- construction
	// ------------------------------------------------------------------------------

	/** Constructs an empty in-memory JSON record */
	public LazyJsonRecord(Schema schema, LazyRecordSerializer serializer) {
		this.schema = schema;
		this.serializer = serializer;
	}

	public void setBuffer(LazyJsonInputBuffer buffer) {
		this.buffer = buffer;
		decoded = false;
		decodedValue = null;
	}
	
	public LazyJsonInputBuffer getBuffer() {
		return buffer;
	}
	
	public Schema getSchema() {
		return schema;
	}
  
	private void ensureRecord() {
		if (!decoded) {
			decodedValue = (JsonRecord) serializer.decode(buffer, null);
			decoded = true;
		}
	}

	@Override
	public boolean containsKey(JsonString key) {
		ensureRecord();
		return decodedValue.containsKey(key);
	}

	@Override
	public JsonValue get(JsonString key, JsonValue defaultValue) {
		ensureRecord();
		return decodedValue.get(key, defaultValue);
	}

	@Override
	public JsonRecord getCopy(JsonValue target) throws Exception {
		ensureRecord();
		return decodedValue.getCopy(target);
	}

	@Override
	public Iterator<Entry<JsonString, JsonValue>> iterator() {
		ensureRecord();
		return decodedValue.iterator();
	}

	@Override
	public Iterator<Entry<JsonString, JsonValue>> iteratorSorted() {
		ensureRecord();
		return decodedValue.iteratorSorted();
	}

	@Override
	public int size() {
		ensureRecord();
		return decodedValue.size();
	}

	@Override
	public JsonEncoding getEncoding() {
		ensureRecord();
		return decodedValue.getEncoding();
	}

	@Override
	public JsonValue getImmutableCopy() throws Exception {
		return this;
	}
  
  /*
  public void set(byte[] data, boolean[] optBits) {
	  this.data = data;
	  this.optBits = optBits;
	  
	  int optIndex = 0;
	  int fieldIndex = 0;
	  
	  for (int i = 0; i < info.length; i++) {
		  if(!info[i].optional) {
			  names[fieldIndex++] = info[i].name;
		  }
		  else if(optBits[i]) {
			  names[fieldIndex++] = info[i].name;
			  optIndex++;
		  } else {
			  optIndex++;
		  }
	}
  }
  */

}

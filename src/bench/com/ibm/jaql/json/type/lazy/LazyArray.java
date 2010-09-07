package com.ibm.jaql.json.type.lazy;

import com.ibm.jaql.io.serialization.binary.perf.lazy.LazyArraySerializer;
import com.ibm.jaql.io.serialization.binary.perf.lazy.LazyJsonInputBuffer;
import com.ibm.jaql.io.serialization.binary.perf.lazy.LazySerializer;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonEncoding;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;

public class LazyArray extends JsonArray {
	
	private final LazySerializer<JsonArray> serializer;
	//private JsonLong decodedValue;
	private LazyJsonInputBuffer buffer;
	private boolean decoded;
	private JsonArray decodedArray;

	public LazyArray(ArraySchema s, LazyArraySerializer serializer) {
		this.serializer = serializer;
		decoded = false;
	}
	
	public void setBuffer(LazyJsonInputBuffer buffer) {
		this.buffer = buffer;
		decoded = false;
		decodedArray = null;
	}
	
	private void ensureArray() {
		if(!decoded) {
			decodedArray = serializer.decode(buffer, null);
			decoded = true;
		}
	}

	@Override
	public long count() {
		ensureArray();
		return decodedArray.count();
	}

	@Override
	public JsonValue get(long n) throws Exception {
		ensureArray();
		return decodedArray.get(n);
	}

	@Override
	public void getAll(JsonValue[] target) throws Exception {
		ensureArray();
		decodedArray.getAll(target);
	}

	@Override
	public JsonIterator iter() throws Exception {
		ensureArray();
		return decodedArray.iter();
	}

	@Override
	public JsonValue getCopy(JsonValue target) throws Exception {
		ensureArray();
		return decodedArray.getCopy(target);
	}

	@Override
	public JsonEncoding getEncoding() {
		//TODO: Is this right?
		ensureArray();
		return decodedArray.getEncoding();
	}

	@Override
	public JsonValue getImmutableCopy() throws Exception {
		return this;
	}

}

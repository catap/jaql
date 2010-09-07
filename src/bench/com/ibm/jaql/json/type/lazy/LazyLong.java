package com.ibm.jaql.json.type.lazy;

import com.ibm.jaql.io.serialization.binary.perf.lazy.LazyJsonInputBuffer;
import com.ibm.jaql.io.serialization.binary.perf.lazy.LazySerializer;
import com.ibm.jaql.json.schema.LongSchema;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonValue;

public class LazyLong extends JsonLong {
	private final LazySerializer<JsonLong> serializer;
	private JsonLong decodedValue;
	private LazyJsonInputBuffer buffer;
	private boolean decoded;

	public LazyLong(LongSchema s, LazySerializer<JsonLong> serializer) {
		super(Long.MIN_VALUE);
		this.serializer = serializer;
		decoded = false;
	}
	
	private final void ensureLong() {
		if(!decoded) {
			decodedValue = serializer.decode(buffer, null);
			decoded = true;
		}
	}

	public void setBuffer(LazyJsonInputBuffer buffer) {
		this.buffer = buffer;
		value = Long.MIN_VALUE; // For debug
		decoded = false;
	}
	
	public LazyJsonInputBuffer getBuffer() {
		return buffer;
	}

	@Override
	public long longValue() {
		ensureLong();
		return decodedValue.longValue();
	}

	/* @see com.ibm.jaql.json.type.JsonNumeric#longValueExact() */
	@Override
	public long longValueExact() {
		ensureLong();
		return decodedValue.longValueExact();
	}
	
	@Override
	public long get() {
		ensureLong();
		return decodedValue.longValueExact();
	}
	
	//TODO: Investigate why a special implementation is possible,
	//this is sovled but could be the reason why others fail
	public String toString() {
		ensureLong();
		return super.toString();
	}
	
	@Override
	public JsonLong getCopy(JsonValue target) {
		LazyLong copy = new LazyLong(null, serializer);
		if(decoded) {
			copy.decoded = true;
			copy.decodedValue = this.decodedValue;
		} else {
			copy.decoded = false;
			copy.buffer = this.buffer.getCopy();
		}
		return copy;
	}

	//TODO: implement get copy and copy buffer so that it cannot be changed by other
	//references. NEEDS TO BE DONE FOR ALL TYPES!!!
}

package com.ibm.jaql.json.type.lazy;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.UndeclaredThrowableException;

import com.ibm.jaql.io.serialization.binary.perf.lazy.LazyJsonInputBuffer;
import com.ibm.jaql.io.serialization.binary.perf.lazy.LazySerializer;
import com.ibm.jaql.io.serialization.binary.perf.lazy.LazyStringSerializer;
import com.ibm.jaql.json.schema.StringSchema;
import com.ibm.jaql.json.type.JsonString;

public class LazyString extends JsonString {

	private final LazySerializer<JsonString> serializer;
	private JsonString decodedValue;
	private LazyJsonInputBuffer buffer;
	private boolean decoded;

	public LazyString(StringSchema s, LazyStringSerializer serializer) {
		this.serializer = serializer;
		decoded = false;
	}

	public void setBuffer(LazyJsonInputBuffer buffer) {
		this.buffer = buffer;
		decoded = false;
		this.bytes = null;
		this.hasBytes = false;
		invalidateCache();
	}

	/** Makes sure that the bytes are computed from the string. */
	@Override
	protected void ensureBytes() {
		if (decoded)
			return;

		decodedValue = serializer.decode(buffer, null);
		bytes = decodedValue.getInternalBytes();
		bytesLength = bytes.length;
		hasBytes = true;
		decoded = true;
	}

	/** Makes sure that the cached Java string is computed from the bytes. */
	protected void ensureString() {
		if (cachedString == null) {
			ensureBytes();
			try {
				cachedString = new String(bytes, 0, bytesLength, "UTF8");
			} catch (UnsupportedEncodingException e) {
				throw new UndeclaredThrowableException(e);
			}
		}
	}
}

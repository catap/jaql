package com.ibm.jaql.io.serialization.binary.perf.lazy;

import com.ibm.jaql.json.type.JsonValue;

public interface LazySerializer<T extends JsonValue> {
	public T decode(LazyJsonInputBuffer in, T target);
}

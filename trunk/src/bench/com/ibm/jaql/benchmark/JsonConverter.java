package com.ibm.jaql.benchmark;

import com.ibm.jaql.json.type.JsonValue;

public interface JsonConverter {
	public Object convert(JsonValue val);
}

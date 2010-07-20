package com.ibm.jaql.benchmark;

import com.ibm.jaql.json.type.JsonValue;

public interface JsonAggregate {
	public void accumulate(JsonValue value) throws Exception;
	public JsonValue getFinal() throws Exception;
}

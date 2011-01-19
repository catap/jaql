package com.ibm.jaql.io.serialization.binary.perf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.json.type.JsonValue;

public interface PerfSerializer<T extends JsonValue> {
	public JsonValue read(DataInput in, JsonValue target) throws IOException;
	public void write(DataOutput out, T inValue) throws IOException;
}

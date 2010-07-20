package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JsonSingleDataBenchmarkProgram;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonString;

public class JsonStringConcat extends JsonSingleDataBenchmarkProgram {
	final JsonString whitespace = new JsonString(" "); 
	JsonValue[] values;
	
	public JsonStringConcat() {
		values = new JsonValue[2];
	}

	@Override
	public JsonValue nextResult(JsonValue val) {
		JsonArray arr = (JsonArray) val;
		try {
			arr.getAll(values);
		} catch (Exception e) {
			throw new RuntimeException("aaaaa");
		}

		JsonString text1 = (JsonString) values[0];
        JsonString text2 = (JsonString) values[1];
        
        MutableJsonString result = new MutableJsonString();
        result.ensureCapacity(text1.bytesLength() + whitespace.bytesLength() + text2.bytesLength());
        byte[] buf = result.get();
        text1.writeBytes(buf, 0, text1.bytesLength());
        whitespace.writeBytes(buf, text1.bytesLength(), whitespace.bytesLength());
        text2.writeBytes(buf, text1.bytesLength() + whitespace.bytesLength(), text2.bytesLength());
        result.set(buf, text1.bytesLength() + whitespace.bytesLength() + text2.bytesLength());
        return result;
	}

}

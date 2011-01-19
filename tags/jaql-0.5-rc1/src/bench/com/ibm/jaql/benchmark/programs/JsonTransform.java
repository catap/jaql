package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JsonBenchmarkProgramSingleInput;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonString;

public class JsonTransform extends JsonBenchmarkProgramSingleInput {
	BufferedJsonRecord rec;
	public static final JsonString NAME_FIELD = new JsonString("name");
	public static final JsonString ID_FIELD = new JsonString("id");
	private final JsonString whitespace = new JsonString(" ");
	private final JsonString forename = new JsonString("forename");
	private final JsonString lastname = new JsonString("lastname");
	private final MutableJsonString result = new MutableJsonString();
	
	public JsonTransform() {
		rec = new BufferedJsonRecord();
		rec.add(new JsonString(NAME_FIELD), new JsonString("invalid"));
		rec.add(new JsonString(ID_FIELD), new JsonDecimal(-1));
	}
	
	@Override
	public JsonValue nextResult(JsonValue val) {
		JsonRecord person = (JsonRecord) val;

		JsonString text1 = (JsonString) person.get(forename);
        JsonString text2 = (JsonString) person.get(lastname);
        
        result.ensureCapacity(text1.bytesLength() + whitespace.bytesLength() + text2.bytesLength());
        byte[] buf = result.get();
        text1.writeBytes(buf, 0, text1.bytesLength());
        whitespace.writeBytes(buf, text1.bytesLength(), whitespace.bytesLength());
        text2.writeBytes(buf, text1.bytesLength() + whitespace.bytesLength(), text2.bytesLength());
        result.set(buf, text1.bytesLength() + whitespace.bytesLength() + text2.bytesLength());
        
		rec.set(NAME_FIELD, result);
		rec.set(ID_FIELD, person.get(ID_FIELD));
		
		return rec;
	}
}

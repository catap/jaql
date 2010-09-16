package com.ibm.jaql.io.stream.converter;


import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import com.ibm.jaql.io.converter.JsonToStream;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.del.JsonToDel;
import com.ibm.jaql.util.SystemUtil;

public class LineTextOutputStream implements JsonToStream<JsonValue> {

	private PrintStream output;
	
	@Override
	public void close() throws IOException {
		if(output != null){
			output.flush();
			output.close();
		}
	}

	@Override
	public boolean isArrayAccessor() {
		return false;
	}

	@Override
	public void setArrayAccessor(boolean a) {
		
	}

	@Override
	public void setOutputStream(OutputStream out) {
		output = new PrintStream(out);
	}

	@Override
	public void write(JsonValue i) throws IOException {
		output.write(i.toString().getBytes());
		output.write(SystemUtil.LINE_SEPARATOR.getBytes());
	}

	@Override
	public void init(JsonValue options) throws Exception {
	}
}

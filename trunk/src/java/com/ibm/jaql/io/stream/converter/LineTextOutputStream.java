package com.ibm.jaql.io.stream.converter;


import java.io.IOException;
import java.io.OutputStream;

import com.ibm.jaql.io.converter.JsonToStream;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.FastPrintStream;

public class LineTextOutputStream implements JsonToStream<JsonValue> {

	private FastPrintStream output;
	
  @Override
  public void flush() throws IOException
  {
    if (output != null)
    {
      output.flush();
    }
  }

	@Override
	public void close() throws IOException {
		if(output != null){
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
		output = new FastPrintStream(out);
	}

	@Override
	public void write(JsonValue i) throws IOException {
		output.println(i.toString());
	}

	@Override
	public void init(JsonValue options) throws Exception {
	}
}

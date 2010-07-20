package com.ibm.jaql.benchmark.io;

import java.io.IOException;

import com.ibm.jaql.io.AbstractOutputAdapter;
import com.ibm.jaql.io.ClosableJsonWriter;
import com.ibm.jaql.json.type.JsonValue;

public class NullJsonOutput extends AbstractOutputAdapter {

	@Override
	public ClosableJsonWriter getWriter() throws Exception {
		return new ClosableJsonWriter() {
			@Override
			public void close() throws IOException {
			}
			
			@Override
			public void write(JsonValue value) throws IOException {
				// Do nothing
			}
		};
	}

}

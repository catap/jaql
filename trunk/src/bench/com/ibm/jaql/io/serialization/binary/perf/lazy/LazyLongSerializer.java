package com.ibm.jaql.io.serialization.binary.perf.lazy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.io.serialization.binary.perf.PerfSerializer;
import com.ibm.jaql.json.schema.LongSchema;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.json.type.lazy.LazyLong;
import com.ibm.jaql.util.BaseUtil;

public final class LazyLongSerializer extends BinaryBasicSerializer<JsonLong>
		implements LazySerializer<JsonLong>, PerfSerializer<JsonLong> {

	private final LongSchema schema;
	private final LazyLong lazy;
	private final MutableJsonLong t = new MutableJsonLong();

	// -- construction
	// ------------------------------------------------------------------------------

	public LazyLongSerializer(LongSchema schema) {
		this.schema = schema;
		this.lazy = new LazyLong(schema, this);
	}

	// -- serialization
	// -----------------------------------------------------------------------------

	@Override
	public JsonLong read(DataInput in, JsonValue target) throws IOException {
		assert in instanceof LazyJsonInputBuffer;
		lazy.setBuffer((LazyJsonInputBuffer) in);
		return lazy;
	}

	@Override
	public void write(DataOutput out, JsonLong value) throws IOException {
		//if(value instanceof LazyLong) {
			//TODO: Compare schema
		//	((LazyLong)value).getBuffer().writeToOutput(out);
		//} else {
			BaseUtil.writeVSLong(out, value.get());
		//}
	}
	
	@Override
	public JsonLong decode(LazyJsonInputBuffer in, JsonLong target) {
		try {
			t.set(readValue(in));
			return t;
		} catch (IOException e) {
			throw new RuntimeException("Error during lazy deserialization");
		}
	}


	private long readValue(DataInput in) throws IOException {
		return BaseUtil.readVSLong(in);
	}

	// -- comparison
	// --------------------------------------------------------------------------------

	public int compare(DataInput in1, DataInput in2) throws IOException {
		if (schema.isConstant()) {
			return 0;
		}

		long value1 = readValue(in1);
		long value2 = readValue(in2);
		return (value1 < value2) ? -1 : (value1 == value2 ? 0 : +1);
	}
}

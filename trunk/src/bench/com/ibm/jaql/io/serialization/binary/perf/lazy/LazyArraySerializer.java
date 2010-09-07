package com.ibm.jaql.io.serialization.binary.perf.lazy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.io.serialization.binary.perf.PerfBinaryFullSerializer;
import com.ibm.jaql.io.serialization.binary.perf.PerfSerializer;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.lazy.LazyArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.util.BaseUtil;

public final class LazyArraySerializer extends BinaryBasicSerializer<JsonArray>
		implements LazySerializer<JsonArray>, PerfSerializer<JsonArray> {
	private ArraySchema schema;
	private PerfSerializer[] headSerializers;
	private PerfSerializer restSerializer;
	private LazyArray lazy;
	private final LazyJsonOutputBuffer outBuffer = new LazyJsonOutputBuffer();
	// -- construction
	// ------------------------------------------------------------------------------

	public LazyArraySerializer(ArraySchema schema) {
		this.schema = schema;
		init();
		lazy = new LazyArray(schema, this);
	}

	private void init() {
		List<Schema> head = schema.getHeadSchemata();
		headSerializers = new PerfSerializer[head.size()];
		for (int i = 0; i < head.size(); i++) {
			headSerializers[i] = PerfBinaryFullSerializer
					.getSerializerBySchema(head.get(i));
		}

		Schema rest = schema.getRestSchema();
		if (rest != null) {
			restSerializer = PerfBinaryFullSerializer.getSerializerBySchema(rest);
		}
	}

	// -- serialization
	// -----------------------------------------------------------------------------

	@Override
	public JsonArray read(DataInput in, JsonValue target) throws IOException {
		assert in instanceof LazyJsonInputBuffer;
		lazy.setBuffer((LazyJsonInputBuffer) in);
		return lazy;
	}

	@Override
	public void write(DataOutput out, JsonArray value) throws IOException {
		try {
			// check the count
			long n = value.count();
			if (n < schema.minElements().get()
					|| (schema.maxElements() != null && n > schema
							.maxElements().get())) {
				throw new IllegalArgumentException(
						"input array has invalid length");
			}

			// get an iterator
			JsonIterator iter = value.iter();

			// serialize the head
			for (PerfSerializer headSerializer : headSerializers) {
				boolean success = iter.moveNext();
				assert success; // guaranteed by checking count above
				headSerializer.write(outBuffer, iter.current());
				BaseUtil.writeVUInt(out, outBuffer.pos);
				out.write(outBuffer.buffer, 0, outBuffer.pos);
				outBuffer.reset();
			}
			n -= headSerializers.length;

			// serialize the rest
			if (restSerializer != null) {
				// serialize the count
				BaseUtil.writeVULong(out, n);

				// write the elements
				for (long i = 0; i < n; i++) {
					boolean success = iter.moveNext();
					assert success; // guaranteed by checking count above
					restSerializer.write(outBuffer, iter.current());
					BaseUtil.writeVUInt(out, outBuffer.pos);
					out.write(outBuffer.buffer, 0, outBuffer.pos);
					outBuffer.reset();
				}
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	// -- comparison
	// --------------------------------------------------------------------------------
	/*
	 * public int compare(DataInput in1, DataInput in2) throws IOException { //
	 * compare the head for (BinaryFullSerializer headSerializer :
	 * headSerializers) { int cmp = headSerializer.compare(in1, in2); if (cmp !=
	 * 0) return cmp; }
	 * 
	 * // compare tails if (restSerializer != null) { // get the count long n1 =
	 * BaseUtil.readVULong(in1); long n2 = BaseUtil.readVULong(in2); return
	 * FullSerializer.compareArrays(in1, n1, in2, n2, restSerializer); }
	 * 
	 * // else they are identical return 0; }
	 */

	public JsonArray decode(LazyJsonInputBuffer in, JsonArray target) {
		//TODO: Use SpillJsonArray when that uses Lazy values
		try {
			// construct target
			// TODO: make type dependent on schema?
			BufferedJsonArray t = new BufferedJsonArray();
			//TODO: Proper solution for inbuffer
			LazyJsonInputBuffer	inBuffer;
			// read the head
			for (int i = 0; i < headSerializers.length; i++) {
				int length = BaseUtil.readVUInt(in);
				inBuffer = new LazyJsonInputBuffer();
				inBuffer = in.readBuffer(inBuffer, length);
				t.add(headSerializers[i].read(inBuffer, null));
			}

			// read the rest
			if (restSerializer != null) {
				// get the count
				long n = BaseUtil.readVULong(in);

				// read the elements
				inBuffer = new LazyJsonInputBuffer();
				for (long i = 0; i < n; i++) {
					int length = BaseUtil.readVUInt(in);
					inBuffer = in.readBuffer(inBuffer, length);
					t.addCopy(restSerializer.read(inBuffer, null));
				}
			}

			// done
			return t;
		} catch (Exception ex) {
			throw new RuntimeException("Error during deserialization", ex);
		}
	}
	/*
	 * @Override public JsonArray decode(LazyJsonInputBuffer in, JsonArray
	 * target) { // construct target // TODO: make type dependent on schema?
	 * SpilledJsonArray t; if (!(target instanceof SpilledJsonArray)) { t = new
	 * SpilledJsonArray(); } else { t = (SpilledJsonArray)target; t.clear(); }
	 * 
	 * // read the head for (int i=0; i<headSerializers.length; i++) {
	 * t.addCopySerialized(in, headSerializers[i]); }
	 * 
	 * // read the rest if (restSerializer != null) { // get the count long n =
	 * BaseUtil.readVULong(in);
	 * 
	 * // read the elements for (long i=0; i<n; i++) { // TODO: use cache here?
	 * t.addCopySerialized(in, restSerializer); } }
	 * 
	 * // done return t; }
	 */
	// TODO: efficient implementation of skip, and copy
}

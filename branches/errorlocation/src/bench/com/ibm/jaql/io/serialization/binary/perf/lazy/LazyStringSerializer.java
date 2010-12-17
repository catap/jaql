/*
 * Copyright (C) IBM Corp. 2008.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.ibm.jaql.io.serialization.binary.perf.lazy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.io.serialization.binary.perf.PerfSerializer;
import com.ibm.jaql.json.schema.StringSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonString;
import com.ibm.jaql.json.type.lazy.LazyString;

public final class LazyStringSerializer extends
		BinaryBasicSerializer<JsonString> implements
		LazySerializer<JsonString>, PerfSerializer<JsonString> {
	private final StringSchema schema;
	private final LazyString lazy;
	private final MutableJsonString str;

	// -- construction
	// ------------------------------------------------------------------------------

	public LazyStringSerializer(StringSchema schema) {
		this.schema = schema;
		str = new MutableJsonString();
		lazy = new LazyString(schema, this);
	}

	// -- serialization
	// -----------------------------------------------------------------------------

	@Override
	public JsonString read(DataInput in, JsonValue ignored) throws IOException {
		//TODO: are lazy strings worth it??
		assert in instanceof LazyJsonInputBuffer;
		lazy.setBuffer((LazyJsonInputBuffer) in);
		return lazy;
	}

	@Override
	public void write(DataOutput out, JsonString value) throws IOException {
		// Schema check is necessary in this case as strings can have additional
		// properties like restriction on length
		if (!schema.matches(value)) {
			throw new IllegalArgumentException(
					"value not matched by this serializer");
		}
		
		value.writeBytes(out);
	}

	@Override
	public JsonString decode(LazyJsonInputBuffer in, JsonString target) {
		byte[] bytes = in.copyBufferContent();
		str.set(bytes);
		return str;
	}

	// -- comparison
	// --------------------------------------------------------------------------------

	/*
	 * public int compare(DataInput in1, DataInput in2) throws IOException { if
	 * (schema.isConstant()) { return 0; }
	 * 
	 * // read length int l1, l2; if (length < 0) { l1 =
	 * BaseUtil.readVUInt(in1); l2 = BaseUtil.readVUInt(in2); } else { l1 = l2 =
	 * length; }
	 * 
	 * // now compare bytes int m = Math.min(l1, l2); for (int i=0; i<m; i++) {
	 * int b1 = in1.readByte(); int b2 = in2.readByte(); if (b1 != b2) { return
	 * (b1 & 0xff) - (b2 & 0xff); } } return l1 - l2; }
	 */
	// TODO: efficient implementation of skip, and copy
}

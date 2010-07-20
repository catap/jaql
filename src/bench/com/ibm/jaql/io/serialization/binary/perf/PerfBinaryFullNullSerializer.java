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
package com.ibm.jaql.io.serialization.binary.perf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.io.serialization.binary.def.DefaultBinaryFullSerializer;
import com.ibm.jaql.json.schema.NullSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonValue;

/** 
 * Jaql's serializer for temporary files. Mainly used for serialization in between the map and
 * reduce phase. The file format is not stable. 
 *
 * This serializer uses an efficient serialization format if schema information is given. This works
 * as follows. 
 * 
 * The provided schema is first broken into basic schemata, i.e., schemata that 
 * represent a basic type but not <code>schema null</code> or <code>schema any</code>. For example, 
 * <code>schema any | long | null | string</code> is broken into the parts 
 * <code>schema long</code> and <code>schema string</code>. For each of the two parts, a basic 
 * serializer is instantiated: <code>LongSerializer</code> and <code>StringSerializer</code>, 
 * in this case. 
 *
 * When a value is written that matches one of the basic schemata, the serializer first writes encoding
 * <code>INDEX_OFFSET+index</code>, where <code>index</code> is a unique index associated with
 * each basic schema. It then uses the associated basic serializer to serialize the value. 
 * 
 * When a value is written that does not match any of the basic schemata, but does match 
 * <code>schema null</code> or <code>schema any</code> (if present), then the value is directly 
 * written using the {@link DefaultBinaryFullSerializer}. This serializer will first write an
 * encoding strictly less than <code>INDEX_OFFSET</code>, followed by the value.
 * 
 * When a value is read, the serializer first reads the encoding from the input stream. If it is
 * greater than or equal to <code>INDEX_OFFSET</code>, a basic schema is matched and the respective 
 * basic serializer is used for reading. Otherwise, the default serializer associated with
 * the encoding just read is used to read the value.
 * 
 * If this class matches only one schema (a single basic schema or null or any), then no type 
 * information is written.
 */
public final class PerfBinaryFullNullSerializer extends BinaryFullSerializer
		implements PerfSerializer<JsonValue> // for the moment
{

	// -- construction
	// ------------------------------------------------------------------------------

	public PerfBinaryFullNullSerializer(Schema schema) {
		if (!(schema instanceof NullSchema)) {
			throw new RuntimeException("Invalid schema for null serializer "
					+ schema);
		}
	}

	// -- full serialization
	// ------------------------------------------------------------------------

	@Override
	public JsonValue read(DataInput in, JsonValue target) throws IOException {
		return null;
	}

	@Override
	public void write(DataOutput out, JsonValue value) throws IOException {
		return;
	}

}

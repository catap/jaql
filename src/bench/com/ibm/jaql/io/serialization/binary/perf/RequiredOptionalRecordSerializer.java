package com.ibm.jaql.io.serialization.binary.perf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.RecordSchema.Field;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
//import com.ibm.jaql.lang.util.JsonStringCache;

public final class RequiredOptionalRecordSerializer extends BinaryBasicSerializer<JsonRecord> implements PerfSerializer<JsonRecord> {
	private final BufferedJsonRecord target;
	private final JsonString[] names;
	private final JsonValue[] values;
	private final int noRequiredOptional;
	private final FieldInfo[] fieldsInfo;
	private final byte[] optBits;
	private final int[] optPosBit;
	
	public RequiredOptionalRecordSerializer(RecordSchema schema) {
		if(schema.hasAdditional()) {
			throw new RuntimeException("No additional fields allowed in this serializer");
		}
		
		noRequiredOptional = schema.noRequiredOrOptional();
		optBits = new byte[(int) Math.ceil(schema.noOptional()/8.0)];
		optPosBit = new int[optBits.length*8];
		for (int i = 0; i < optPosBit.length; i++) {
			optPosBit[i] = (1 << i % 8);
		}
		
		target = new BufferedJsonRecord();
		target.ensureCapacity(noRequiredOptional);
		names = target.getInternalNamesArray();
		values = target.getInternalValuesArray();
		
		fieldsInfo = new FieldInfo[noRequiredOptional];
		for (int i = 0; i < noRequiredOptional; i++) {
			Field field = schema.getFieldByName(i);
			fieldsInfo[i] = new FieldInfo(field, 
					PerfBinaryFullSerializer.getSerializerBySchema(field.getSchema()));
		}
	}
	
	@Override
	public JsonRecord read(DataInput in, JsonValue ignored) throws IOException {
		int optIndex = 0;
		int fieldIndex = 0;
		in.readFully(optBits);

		for (int i = 0; i < fieldsInfo.length; i++) {
			FieldInfo f = fieldsInfo[i];
			if(!f.optional ) {
				names[fieldIndex] = f.name;
				values[fieldIndex] = f.serializer.read(in, null);
				fieldIndex++;
			}
			else if((optBits[optIndex/8] & optPosBit[optIndex]) != 0) {
				names[fieldIndex] = f.name;
				values[fieldIndex] = f.serializer.read(in, null);
				fieldIndex++;
				optIndex++;
			} else {
				optIndex++;
			}
		}
		
		target.set(names, values, fieldIndex, true);
		return target;
	}

	@Override
	public void write(DataOutput out, JsonRecord value) throws IOException {
		int optIndex = 0;
		int outIndex = 0;
		Arrays.fill(optBits, (byte)0);
		
		JsonValue[] outValues = new JsonValue[values.length];
		PerfSerializer[] outSerializers = new PerfSerializer[values.length];
		
		Iterator<Entry<JsonString, JsonValue>> it = value.iteratorSorted();
		//TODO: Handle empty records
		Entry<JsonString, JsonValue> entry = it.next();
		JsonString name = entry.getKey();
		JsonValue val = entry.getValue();
		for (int i = 0; i < fieldsInfo.length; i++) {
			if(fieldsInfo[i].optional) {
				if(fieldsInfo[i].name.equals(name)) {
					//Set bit to present
					optBits[optIndex / 8] = (byte) (optBits[optIndex / 8] | (1 << optIndex % 8));
					outValues[outIndex] = val;
					outSerializers[outIndex] = fieldsInfo[i].serializer;
					outIndex++;
					if(!it.hasNext()) name=null;
					else {
						it.next();
						name = entry.getKey();
						val = entry.getValue();
					}
				} else {
					//Is Automatically zero
					//optBits[optIndex / 8] = (byte) (optBits[optIndex / 8] & (0 << optIndex % 8));
				}
				optIndex++;
			} else if (fieldsInfo[i].name.equals(name)) {
				outValues[outIndex] = val;
				outSerializers[outIndex] = fieldsInfo[i].serializer;
				outIndex++;
				if(!it.hasNext()) name=null;
				else {
					it.next();
					name = entry.getKey();
					val = entry.getValue();
				}
			} else {
				//Field is not optional but not present
			}
		}
		
		out.write(optBits, 0, optBits.length);
		for (int i = 0; i < outIndex; i++) {
			outSerializers[i].write(out, outValues[i]);
		}
	}
	
	private static class FieldInfo {
		final PerfSerializer serializer;
		final JsonString name;
		final boolean optional;

		FieldInfo(RecordSchema.Field field, PerfSerializer<?> serializer) {
			this.serializer = serializer;
			this.name = field.getName(); //JsonStringCache.get(field.getName());
			this.optional = field.isOptional();
		}
	}

}

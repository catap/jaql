package com.ibm.jaql.io.serialization.binary.perf.lazy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.io.serialization.binary.perf.PerfBinaryFullSerializer;
import com.ibm.jaql.io.serialization.binary.perf.PerfSerializer;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.RecordSchema.Field;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.lazy.LazyJsonRecord;
import com.ibm.jaql.util.BaseUtil;

public final class LazyRecordSerializer extends BinaryBasicSerializer<JsonRecord> implements LazySerializer<JsonValue>, PerfSerializer<JsonRecord> {
	private final LazyJsonRecord lazy;
	private final int noRequiredOptional;
	private final int noRequired;
	private final int noOptional;
	private final FieldInfo[] info;
	private final byte[] optBits;
	private final int[] optPosBit;
	private final LazyJsonOutputBuffer out = new LazyJsonOutputBuffer();
	private final boolean[] optBooleans;
	private final BufferedJsonRecord target;
	private final RecordSchema schema;
	private LazyJsonInputBuffer inBuffer = new LazyJsonInputBuffer();
	private HashSet<Integer> compareCache = new HashSet<Integer>();
	
	public LazyRecordSerializer(RecordSchema schema) {
		this.schema = schema;
		if(schema.hasAdditional()) {
			throw new RuntimeException("No additional fields allowed in this serializer");
		}
		noRequired = schema.noRequired();
		noOptional = schema.noOptional();
		noRequiredOptional = schema.noRequiredOrOptional();
		optBits = new byte[(int) Math.ceil(noOptional/8.0)];
		optPosBit = new int[optBits.length*8];
		optBooleans = new boolean[optBits.length*8];
		for (int i = 0; i < optPosBit.length; i++) {
			optPosBit[i] = (1 << i % 8);
		}
		
		info = new FieldInfo[noRequiredOptional];
		for (int i = 0; i < noRequiredOptional; i++) {
			Field field = schema.getFieldByName(i);
			info[i] = new FieldInfo(field, 
					PerfBinaryFullSerializer.getSerializerBySchema(field.getSchema()));
		}
		
		target = new BufferedJsonRecord();
		lazy= new LazyJsonRecord(schema, this);
	}
	
	@Override
	public JsonRecord read(DataInput in, JsonValue ignored) throws IOException {
		assert in instanceof LazyJsonInputBuffer;
		lazy.setBuffer((LazyJsonInputBuffer) in);
		return lazy;
	}

	@Override
	public void write(DataOutput outOriginal, JsonRecord value) throws IOException {
		if(value instanceof LazyJsonRecord) {
			LazyJsonRecord rec = (LazyJsonRecord) value;
			boolean identical = false;
			if(compareCache.contains(System.identityHashCode(rec))) {
				identical = true;
			}
			else if (schema.equals(rec.getSchema())) {
				identical = true;
				compareCache.add(System.identityHashCode(rec));	
			}
			
			if(identical) {
				((LazyJsonRecord)value).getBuffer().writeToOutput(outOriginal);
				return;
			}
		}
		
		//TODO: It is not checked whether it is a valid record!!
		//Count if all required are existing (just check number of required fields writen and defined
		//Count total number of fields written and check wether there were any non defined fields
		int optIndex = 0;
		int outIndex = 0;
		Arrays.fill(optBits, (byte)0);
		
		JsonValue[] outValues = new JsonValue[info.length];
		PerfSerializer[] outSerializers = new PerfSerializer[info.length];
		
		Iterator<Entry<JsonString, JsonValue>> it = value.iteratorSorted();
		//TODO: Handle empty records
		Entry<JsonString, JsonValue> entry = it.next();
		JsonString name = entry.getKey();
		JsonValue val = entry.getValue();
		for (int i = 0; i < info.length; i++) {
			if(info[i].optional) {
				if(info[i].name.equals(name)) {
					//Set bit to present
					optBits[optIndex / 8] = (byte) (optBits[optIndex / 8] | (1 << optIndex % 8));
					outValues[outIndex] = val;
					outSerializers[outIndex] = info[i].serializer;
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
			} else if (info[i].name.equals(name)) {
				outValues[outIndex] = val;
				outSerializers[outIndex] = info[i].serializer;
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
		
		outOriginal.write(optBits, 0, optBits.length);
		
		for (int i = 0; i < outIndex; i++) {
			outSerializers[i].write(out, outValues[i]);
			
			BaseUtil.writeVSInt(outOriginal, out.pos);
			outOriginal.write(out.buffer, 0, out.pos);
			out.reset();
		}	
	}
	
	public class FieldInfo {
		public final PerfSerializer serializer;
		public final JsonString name;
		public final boolean optional;

		FieldInfo(RecordSchema.Field field, PerfSerializer<?> serializer) {
			this.serializer = serializer;
			this.name = field.getName(); //JsonStringCache.get(field.getName());
			this.optional = field.isOptional();
		}
	}

	@Override
	public JsonValue decode(LazyJsonInputBuffer in, JsonValue ignored) {
		try {
			in.readFully(optBits);
			
			int count = noRequired;
			for (int i = 0; i < noOptional; i++) {
				optBooleans[i] = (optBits[i/8] & optPosBit[i])!=0?true:false;
				if(optBooleans[i]) count++;
			}
			target.ensureCapacity(count);
			
			int optIndex = 0;
			int fieldIndex = 0;
			JsonString[] names = target.getInternalNamesArray();
			JsonValue[] values = target.getInternalValuesArray();
			  
			for (int i = 0; i < info.length; i++) {
				if (!info[i].optional || optBooleans[optIndex]) {
					names[fieldIndex] = info[i].name;
					int length = BaseUtil.readVSInt(in);
					inBuffer = in.readBuffer(new LazyJsonInputBuffer(), length);
					values[fieldIndex] = info[i].serializer.read(inBuffer, null);
					fieldIndex++;
				}
				
				if(!info[i].optional) {
					optIndex++;
				}
			}
			
			target.set(names, values, count);
		} catch (IOException ex) {
			throw new RuntimeException("Error during deserialization", ex);
		}
		
		return target;
	}
}

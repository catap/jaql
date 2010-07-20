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
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.LazyJsonRecord;
//import com.ibm.jaql.lang.util.JsonStringCache;
import com.ibm.jaql.util.BaseUtil;

public final class LazyRecordSerializer extends BinaryBasicSerializer<JsonRecord> implements PerfSerializer<JsonRecord> {
	private final LazyJsonRecord target;
	private final int noRequiredOptional;
	private final FieldInfo[] fieldsInfo;
	private final byte[] optBits;
	private final int[] optPosBit;
	private final DataOutputBuffer out = new DataOutputBuffer();
	private final boolean[] optBooleans;
	
	public LazyRecordSerializer(RecordSchema schema) {
		if(schema.hasAdditional()) {
			throw new RuntimeException("No additional fields allowed in this serializer");
		}
		
		noRequiredOptional = schema.noRequiredOrOptional();
		optBits = new byte[(int) Math.ceil(schema.noOptional()/8.0)];
		optPosBit = new int[optBits.length*8];
		optBooleans = new boolean[optBits.length*8];
		for (int i = 0; i < optPosBit.length; i++) {
			optPosBit[i] = (1 << i % 8);
		}
		
		fieldsInfo = new FieldInfo[noRequiredOptional];
		for (int i = 0; i < noRequiredOptional; i++) {
			Field field = schema.getFieldByName(i);
			fieldsInfo[i] = new FieldInfo(field, 
					PerfBinaryFullSerializer.getSerializerBySchema(field.getSchema()));
		}
		
		target = new LazyJsonRecord(schema, fieldsInfo);
	}
	
	@Override
	public JsonRecord read(DataInput in, JsonValue ignored) throws IOException {
		long size = BaseUtil.readVSLong(in);
		byte[] data = new byte[(int) size - optBits.length];
		in.readFully(optBits);
		
		for (int i = 0; i < optBooleans.length; i++) {
			optBooleans[i] = (optBits[i/8] & optPosBit[i])!=0?true:false;
		}
		in.readFully(data);
		
		target.set(data, optBooleans);
		return target;
	}

	@Override
	public void write(DataOutput outOriginal, JsonRecord value) throws IOException {
		out.pos = 0;
		
		int optIndex = 0;
		int outIndex = 0;
		Arrays.fill(optBits, (byte)0);
		
		JsonValue[] outValues = new JsonValue[fieldsInfo.length];
		PerfSerializer[] outSerializers = new PerfSerializer[fieldsInfo.length];
		
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
		
		BaseUtil.writeVSLong(outOriginal, out.pos);
		outOriginal.write(out.buffer, 0, out.pos);
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
	
	private static class DataOutputBuffer implements DataOutput{
		int pos = 0;
		final byte[] buffer = new byte[64 * 1024];
		@Override
		public void write(int b) throws IOException {
			throw new RuntimeException("unimplemented");
		}

		@Override
		public void write(byte[] b) throws IOException {
			throw new RuntimeException("unimplemented");
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			System.arraycopy(b, off, buffer, pos, len);
			pos += len;
		}

		@Override
		public void writeBoolean(boolean v) throws IOException {
			throw new RuntimeException("unimplemented");
		}

		@Override
		public void writeByte(int v) throws IOException {
			buffer[pos++] = (byte) (v &0xFF);
		}

		@Override
		public void writeBytes(String s) throws IOException {
			throw new RuntimeException("unimplemented");
		}

		@Override
		public void writeChar(int v) throws IOException {
			throw new RuntimeException("unimplemented");
		}

		@Override
		public void writeChars(String s) throws IOException {
			throw new RuntimeException("unimplemented");
		}

		@Override
		public void writeDouble(double v) throws IOException {
			throw new RuntimeException("unimplemented");
		}

		@Override
		public void writeFloat(float v) throws IOException {
			throw new RuntimeException("unimplemented");
		}

		@Override
		public void writeInt(int v) throws IOException {
			throw new RuntimeException("unimplemented");
		}

		@Override
		public void writeLong(long v) throws IOException {
			throw new RuntimeException("unimplemented");
		}

		@Override
		public void writeShort(int v) throws IOException {
			throw new RuntimeException("unimplemented");
		}

		@Override
		public void writeUTF(String s) throws IOException {
			throw new RuntimeException("unimplemented");
		}
		
	}

}

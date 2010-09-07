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

import com.ibm.jaql.io.serialization.SerializerMap;
import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.io.serialization.binary.def.DefaultBinaryFullSerializer;
import com.ibm.jaql.io.serialization.binary.perf.lazy.LazyArraySerializer;
import com.ibm.jaql.io.serialization.binary.perf.lazy.LazyJsonInputBuffer;
import com.ibm.jaql.io.serialization.binary.perf.lazy.LazyJsonOutputBuffer;
import com.ibm.jaql.io.serialization.binary.perf.lazy.LazyLongSerializer;
import com.ibm.jaql.io.serialization.binary.perf.lazy.LazyRecordSerializer;
import com.ibm.jaql.io.serialization.binary.perf.lazy.LazyStringSerializer;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.GenericSchema;
import com.ibm.jaql.json.schema.LongSchema;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.schema.StringSchema;
import com.ibm.jaql.json.type.JsonEncoding;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.BaseUtil;

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
public final class PerfBinaryFullSerializer extends BinaryFullSerializer implements PerfSerializer<JsonValue> // for the moment
{
  // -- private variables -------------------------------------------------------------------------
  
  /** serializer used for "nonnull" and "null" schemata */  
  private DefaultBinaryFullSerializer defaultSerializer = DefaultBinaryFullSerializer.getInstance();
  
  /** all values below INDEX_OFFSET are assumed to be reserved for the encodings of the default 
   * serializer that is used when no schema information is given. */
  private final int INDEX_OFFSET = 64;
  
  /** holds the schema associated with this serializer */
  private Schema schema;
  
  /** Describes the basic serializers */
  private MySerializerMap serializers;
  
  private final boolean basicTypeSchema;
  private final boolean basicNullSchema;
  private final boolean basicNonNullSchema;
  private BinaryBasicSerializer<JsonValue> basicTypeSerializer = null;
  
  private final LazyJsonInputBuffer lazyInputBuffer;
  private byte[] rawBuffer;
  private final LazyJsonOutputBuffer lazyOutputBuffer;
  
  // -- construction ------------------------------------------------------------------------------
  
	public PerfBinaryFullSerializer(Schema schema) {
		assert JsonEncoding.LIMIT < INDEX_OFFSET; // these values are used for
													// null/nonnull
		// remove duplicates and make sure each type occurs at most once
		this.schema = SchemaTransformation.compact(schema);
		serializers = new MySerializerMap(this.schema);

		basicTypeSchema = !serializers.hasAlternatives();
		basicNullSchema = basicTypeSchema && serializers.matchesNull();
		basicNonNullSchema = basicTypeSchema && serializers.matchesNonNull();
		if (basicTypeSchema && !basicNullSchema && !basicNonNullSchema) {
			basicTypeSerializer = (BinaryBasicSerializer<JsonValue>) makeBasicSerializer(schema);
		} else {
			throw new RuntimeException("Not supported");
		}
		
		lazyInputBuffer = new LazyJsonInputBuffer();
		rawBuffer = new byte[64*1024];
		lazyOutputBuffer = new LazyJsonOutputBuffer();
	}
  

  // -- full serialization ------------------------------------------------------------------------

	@Override
	public JsonValue read(DataInput in, JsonValue target) throws IOException {
		//Read length
		int length = BaseUtil.readVSInt(in);
		//Read buffer
		//TODO: Check buffer size
		in.readFully(rawBuffer, 0, length);
		lazyInputBuffer.setBuffer(rawBuffer, 0, length);

		//Let LazyBuffer be wrapped by serializer into a json datatype
		if (basicTypeSchema) {
			if (basicNullSchema) {
				return null;
			} else if (basicNonNullSchema) {
				// matches nonnull only
				throw new RuntimeException("Not supported");
				//return defaultSerializer.read(in, null);
			} else {
				return basicTypeSerializer.read(lazyInputBuffer, null);
			}
		} else {
			// there are multiple serializers --> get the encoding
			int encoding = BaseUtil.readVUInt(in);
			BinaryBasicSerializer<?> serializer = getSerializer(encoding);
			return serializer.read(in, null);
		}
	}

  @SuppressWarnings("unchecked")
  @Override
  public void write(DataOutput out, JsonValue value) throws IOException
  {
    // handle nulls
    if (value == null)
    {
    	if(basicNullSchema) {
    		return;
    	}
    	else if (serializers.matchesNull() && serializers.hasAlternatives())
    	{
    		defaultSerializer.write(out, value);
    	}
    	throw new IllegalArgumentException("null value is not matched by this serializer");
    }
    
    if(basicTypeSchema) {
    	lazyOutputBuffer.reset();
    	basicTypeSerializer.write(lazyOutputBuffer, value);
    	BaseUtil.writeVSInt(out, lazyOutputBuffer.pos);
    	out.write(lazyOutputBuffer.buffer, 0, lazyOutputBuffer.pos);
    } else {
        // find a serializer
        SerializerInfo k = serializers.get(value);
        if (k == null)
        {
          // none found
          if (serializers.matchesNonNull()) 
          {
            // will write encoding < INDEX_OFFSET
            defaultSerializer.write(out, value);
            return;
          }
          throw new IllegalArgumentException("value " + value + " does not match this serializer's " +
          		"schema " + schema);
        }
        else
        {
          // at least one found
          if (serializers.hasAlternatives())
          {
            // will write encoding >= INDEX_OFFSET
            BaseUtil.writeVUInt(out, k.encoding);
          }
          k.serializer.write(out, value);
        }
    }
  }

  
  // -- basic serializers -------------------------------------------------------------------------

  /** Creates a basic serializer for the specified schema. Used at initialization time. */
  private static BinaryBasicSerializer<?> makeBasicSerializer(Schema schema)
  {
	  if(schema.isConstant()) {
		  return new ConstSerializer(schema);
	  }
    switch (schema.getSchemaType())
    {
    case BOOLEAN:
      //return new BooleanSerializer((BooleanSchema)schema);
    	return null;
    case LONG:
      return new LazyLongSerializer((LongSchema)schema);
    case DECFLOAT:
      //return new DecfloatSerializer((DecfloatSchema)schema);
    	return null;
    case DOUBLE:
      //return new DoubleSerializer((DoubleSchema)schema);
    	return null;
    case STRING:
      return new LazyStringSerializer((StringSchema)schema);
    case BINARY:
      //return new BinarySerializer((BinarySchema)schema);
    	return null;
    case DATE:
      //return new DateSerializer((DateSchema)schema);
    	return null;
    case ARRAY:
    	return new LazyArraySerializer((ArraySchema)schema);
    case RECORD:
    	//if(((RecordSchema) schema).hasAdditional()) {
    	//	return new RecordSerializer((RecordSchema) schema);
    	//} else {
    	//	return new RequiredOptionalRecordSerializer((RecordSchema)schema);
    	return new LazyRecordSerializer((RecordSchema) schema);
    	//}
    case SCHEMATYPE:
      //return new SchemaSerializer((SchematypeSchema)schema);
    	return null;
    case FUNCTION:
      //return DefaultBinaryFullSerializer.getInstance().getSerializer(JsonEncoding.FUNCTION);
    	return null;
    case GENERIC:
      // "function" and "aschema" falls into this category
      // currently, there is only one encoding for those types; checked below
      // DefaultBinaryFullSerializer is used to write them
      JsonType type = ((GenericSchema)schema).getType();
      JsonEncoding encoding = null;
      for (JsonEncoding e : JsonEncoding.values())
      {
        if (e.getType().equals(type)) 
        {
          if (encoding == null)
          {
            encoding = e;
          }
          else
          {
            throw new IllegalStateException("generic schemata are requried to have types with only one encoding");
          }
        }
      }
      assert encoding != null; // every type has an encoding
      return DefaultBinaryFullSerializer.getInstance().getSerializer(encoding);
    case NULL:            // handled separately
    case NON_NULL:    // handled separately
    case OR:              // has been unrolled
      throw new IllegalArgumentException("implementation error");
    default:
      throw new IllegalArgumentException("unsupported schema type");
    }
  }
  
  
  /** Returns the basic serializer that belongs to the specified encoding */
  private BinaryBasicSerializer<?> getSerializer(int encoding)
  {
    if (encoding < INDEX_OFFSET)
    {
      // encoded using default serializer
      return defaultSerializer.getSerializer(JsonEncoding.getEncoding(encoding)); 
    }
    else
    {
      // encoded using one of our serializers
      return serializers.get(encoding-INDEX_OFFSET).serializer;
    }
  }
  
  // -- comparison --------------------------------------------------------------------------------
  
  @SuppressWarnings("unchecked")
  @Override
  public int compare(DataInput in1, DataInput in2) throws IOException 
  {
    if (!serializers.hasAlternatives())
    {
      // there is just one serializer
      if (serializers.matchesNull())
      {
        // matches null only
        return 0;
      }
      else if (serializers.matchesNonNull())
      {
        // matches nonnull only
        return defaultSerializer.compare(in1, in2);
      }
      else
      {
        // matches some other schema --> retrieve and read
        return serializers.get(0).serializer.compare(in1, in2);
      }
    }
    else
    {
      // there are multiple serializers --> get the encoding
      int encoding1 = BaseUtil.readVUInt(in1);
      int encoding2 = BaseUtil.readVUInt(in2);
      
      if (encoding1 == encoding2)
      {
        BinaryBasicSerializer<?> serializer = getSerializer(encoding1);
        return serializer.compare(in1, in2);
      }
      else
      {
        // try to determine JSON types
        boolean default1 = encoding1<INDEX_OFFSET;
        boolean default2 = encoding2<INDEX_OFFSET;
        JsonType type1;
        if (default1)
        {
          type1 = JsonEncoding.getEncoding(encoding1).getType();
        }
        else
        {
          type1 = serializers.get(encoding1-INDEX_OFFSET).schema.getSchemaType().getJsonType();
        }
        JsonType type2;
        if (default2)
        {
          type2 = JsonEncoding.getEncoding(encoding2).getType();
        }
        else
        {
          type2 = serializers.get(encoding2-INDEX_OFFSET).schema.getSchemaType().getJsonType();
        }

        // compare the types
        if (type1 != null && type2 != null)
        {
          int cmp = type1.compareTo(type2);
          if (cmp != 0) return cmp;
        }
        
        // otherwise: give up and deserialize
        BinaryBasicSerializer<?> serializer1 = getSerializer(encoding1);
        BinaryBasicSerializer<?> serializer2 = getSerializer(encoding2);
        JsonValue value1 = serializer1.read(in1, null);
        JsonValue value2 = serializer2.read(in2, null);
        return JsonUtil.compare(value1, value2);
      }
    }   
  }
  
  
  // -- helper classes ----------------------------------------------------------------------------
  
  /** Describes the information associated with serializers */
  private final class SerializerInfo
  {
    final int encoding;
    final Schema schema;
    @SuppressWarnings("unchecked")
    final BinaryBasicSerializer serializer;
    
    SerializerInfo(int index, Schema schema, BinaryBasicSerializer<?> serializer)
    {
      this.encoding = INDEX_OFFSET + index;
      this.schema = schema;
      this.serializer = serializer;
    }
  }
  
  /** Implementation of SerializerMap for the above SerializerInfo class */
  private final class MySerializerMap extends SerializerMap<SerializerInfo>
  {
    public MySerializerMap(Schema schema)
    {
      super(schema);
    }
    
    @Override
    public SerializerInfo makeSerializerInfo(int pos, Schema schema)
    {
      return new SerializerInfo(pos, schema, makeBasicSerializer(schema));
    }
    
    @Override
    public Schema schemaOf(SerializerInfo info)
    {
      return info.schema;
    }    
  }

	public static PerfSerializer<?> getSerializerBySchema(Schema schema) {
		if (schema.isConstant()) {
			return new ConstSerializer(schema);
		}
		
		switch (schema.getSchemaType()) {
		case BOOLEAN:
			// return new BooleanSerializer((BooleanSchema)schema);
			return null;
		case LONG:
			return new LazyLongSerializer((LongSchema) schema);
		case DECFLOAT:
			// return new DecfloatSerializer((DecfloatSchema)schema);
			return null;
		case DOUBLE:
			// return new DoubleSerializer((DoubleSchema)schema);
			return null;
		case STRING:
			return new LazyStringSerializer((StringSchema) schema);
		case BINARY:
			// return new BinarySerializer((BinarySchema)schema);
			return null;
		case DATE:
			// return new DateSerializer((DateSchema)schema);
			return null;
		case ARRAY:
			return new LazyArraySerializer((ArraySchema)schema);
		case RECORD:
			//return new RequiredOptionalRecordSerializer((RecordSchema) schema);
			return new LazyRecordSerializer((RecordSchema) schema);
		case SCHEMATYPE:
			// return new SchemaSerializer((SchematypeSchema)schema);
			return null;
		case FUNCTION:
			// return
			// DefaultBinaryFullSerializer.getInstance().getSerializer(JsonEncoding.FUNCTION);
			return null;
		case GENERIC:
			// "function" and "aschema" falls into this category
			// currently, there is only one encoding for those types; checked
			// below
			// DefaultBinaryFullSerializer is used to write them
			JsonType type = ((GenericSchema) schema).getType();
			JsonEncoding encoding = null;
			for (JsonEncoding e : JsonEncoding.values()) {
				if (e.getType().equals(type)) {
					if (encoding == null) {
						encoding = e;
					} else {
						throw new IllegalStateException(
								"generic schemata are requried to have types with only one encoding");
					}
				}
			}
			assert encoding != null; // every type has an encoding
			//return DefaultBinaryFullSerializer.getInstance().getSerializer(
				//	encoding);
		case NULL: // handled separately
		case NON_NULL: // handled separately
		case OR: // has been unrolled
			throw new IllegalArgumentException("implementation error");
		default:
			throw new IllegalArgumentException("unsupported schema type");
		}
	}

}

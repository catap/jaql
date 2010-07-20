package com.ibm.jaql.io.serialization.binary.perf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.json.schema.LongSchema;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.util.BaseUtil;

public final class LongSerializer extends BinaryBasicSerializer<JsonLong> implements PerfSerializer<JsonLong> {

	  private final LongSchema schema;
	  private final MutableJsonLong t = new MutableJsonLong(); 
	  
	  // -- construction ------------------------------------------------------------------------------
	  
	  public LongSerializer(LongSchema schema)
	  {
	    this.schema = schema; 
	  }
	  
	  // -- serialization -----------------------------------------------------------------------------

	  @Override
	  public JsonLong read(DataInput in, JsonValue target) throws IOException
	  {	    
	    long value = readValue(in);
	    t.set(value);
	    return t;
	  }


	  @Override
	  public void write(DataOutput out, JsonLong value) throws IOException
	  {
		  BaseUtil.writeVSLong(out, value.get());
	  }
	  
	  private long readValue(DataInput in) throws IOException
	  {
		 return BaseUtil.readVSLong(in);
	  }
	  
	  // -- comparison --------------------------------------------------------------------------------
	  
	  public int compare(DataInput in1, DataInput in2) throws IOException {
	    if (schema.isConstant())
	    {
	      return 0;
	    }
	    
	    long value1 = readValue(in1);
	    long value2 = readValue(in2);
	    return (value1 < value2) ? -1 : (value1==value2 ? 0 : +1);
	  }
	  
	  // TODO: efficient implementation of skip, and copy
}

package com.ibm.jaql.io.serialization.binary.perf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonValue;

public class ConstSerializer extends BinaryBasicSerializer<JsonValue> implements PerfSerializer<JsonValue> {
	
	/*
	 * This is the same implementation than used before. Still it needs to be chacked
	 * that the constant is always immuatable and not changed later!
	 */
	  private final JsonValue constant;
	  
	  // -- construction ------------------------------------------------------------------------------
	  
	  public ConstSerializer(Schema schema)
	  {
		  if(!schema.isConstant()) {
			  throw new RuntimeException("Only constant schema allowed");
		  }
		  this.constant = schema.getConstant();
	  }

	@Override
	public JsonValue read(DataInput in, com.ibm.jaql.json.type.JsonValue target)
			throws IOException {
		return constant;
	}

	@Override
	public void write(DataOutput out, JsonValue value) throws IOException {
		return;
	}

}

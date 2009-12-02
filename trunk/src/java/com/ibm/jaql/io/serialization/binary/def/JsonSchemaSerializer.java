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
package com.ibm.jaql.io.serialization.binary.def;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonSchema;

/** Binary serializer a Json schema. */ 
// TODO: The current implementation simply makes use of the text representation of the schema.
//       It might be more efficient to use a real binary format, if needed.
class JsonSchemaSerializer extends BinaryBasicSerializer<JsonSchema> 
{
  @Override
  public JsonSchema read(DataInput in, JsonValue target) throws IOException
  {
    String s = in.readUTF();
    Schema schema = SchemaFactory.parse(s);
    if (target == null || !(target instanceof MutableJsonSchema)) {
      return new MutableJsonSchema(schema);
    } else {
      MutableJsonSchema t = (MutableJsonSchema)target;
      t.set(schema);
      return t;
    }
  }

  @Override
  public void write(DataOutput out, JsonSchema value) throws IOException
  {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    PrintStream pout = new PrintStream(bout);
    com.ibm.jaql.io.serialization.text.def.SchemaSerializer.write(pout, value.get(), 0);
    pout.flush();
    String s = bout.toString();
    out.writeUTF(s);
  }
}

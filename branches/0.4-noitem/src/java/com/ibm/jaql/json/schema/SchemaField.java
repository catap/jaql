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
package com.ibm.jaql.json.schema;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.io.serialization.binary.def.DefaultBinaryFullSerializer;
import com.ibm.jaql.json.type.JsonEncoding;
import com.ibm.jaql.json.type.JsonString;

/** Schema that matches a field ((name, value)-pair) where the value matches its 
 * specified schema.   
 * 
 */
@SuppressWarnings("unchecked")
public class SchemaField
{

  protected final static BinaryBasicSerializer<JsonString> serializer 
  = (BinaryBasicSerializer<JsonString>)DefaultBinaryFullSerializer.getInstance().getSerializer(JsonEncoding.STRING);

  protected SchemaField nextField;
  protected JsonString  name;
  protected boolean     optional;
  protected Schema      schema;

  /**
   * 
   */
  public SchemaField()
  {
  }

  public SchemaField(String name, boolean optional, Schema schema)
  {
    this.name = new JsonString(name);
    this.optional = optional;
    this.schema = schema;
  }

  /**
   * @param in
   * @throws IOException
   */
  public SchemaField(DataInput in) throws IOException
  {
    name = serializer.read(in, name);
    optional = in.readBoolean();
    schema = Schema.read(in);
  }

  /**
   * @param out
   * @throws IOException
   */
  public void write(DataOutput out) throws IOException
  {
    serializer.write(out, name);
    out.writeBoolean(optional);
    schema.write(out);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString()
  {
    String str = "";
    if (name != null)
    {
      str += name;
    }
    if (optional)
    {
      str += " ?";
    }
    str += ": ";
    str += schema.toString();
    return str;
  }

}

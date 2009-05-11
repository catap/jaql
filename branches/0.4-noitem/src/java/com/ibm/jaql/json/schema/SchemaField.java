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

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.io.serialization.def.DefaultFullSerializer;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.Item.Encoding;

/** Schema that matches a field ((name, value)-pair) where the value matches its 
 * specified schema.   
 * 
 */
@SuppressWarnings("unchecked")
public class SchemaField
{

  protected final static BasicSerializer<JString> serializer 
  = (BasicSerializer<JString>)DefaultFullSerializer.getDefaultInstance().getSerializer(Encoding.STRING);

  public SchemaField nextField;
  public JString     name;
  public boolean     wildcard;
  public boolean     optional;
  public Schema      schema;

  /**
   * 
   */
  public SchemaField()
  {
  }

  /**
   * @param in
   * @throws IOException
   */
  public SchemaField(DataInput in) throws IOException
  {
    name = serializer.read(in, name);
    wildcard = in.readBoolean();
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
    out.writeBoolean(wildcard);
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
    // cannot be both wildcard and optional
    assert !wildcard || !optional;

    String str = "";
    if (name != null)
    {
      str += name;
    }
    if (wildcard)
    {
      str += "*";
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

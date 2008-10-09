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

import com.ibm.jaql.json.type.JString;

/**
 * 
 */
public class SchemaField
{
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
    name.readFields(in);
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
    name.write(out);
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

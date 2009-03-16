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

import com.ibm.jaql.json.type.Item;

/**
 * 
 */
public class SchemaOr extends Schema
{
  protected Schema schemaList;

  /**
   * 
   */
  public SchemaOr()
  {
  }

  /**
   * @param s1
   * @param s2
   */
  public SchemaOr(Schema s1, Schema s2)
  {
    addSchema(s1);
    addSchema(s2);
  }

  /**
   * @param in
   * @throws IOException
   */
  public SchemaOr(DataInput in) throws IOException
  {
    Schema s;
    while ((s = Schema.read(in)) != null)
    {
      addSchema(s);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.schema.Schema#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException
  {
    out.writeByte(OR_TYPE);
    for (Schema s = schemaList; s != null; s = s.nextSchema)
    {
      s.write(out);
    }
    out.writeByte(UNKNOWN_TYPE);
  }

  /**
   * @param s
   */
  public void addSchema(Schema s)
  {
    s.nextSchema = schemaList;
    schemaList = s;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.schema.Schema#matches(com.ibm.jaql.json.type.Item)
   */
  @Override
  public boolean matches(Item item) throws Exception
  {
    for (Schema s = schemaList; s != null; s = s.nextSchema)
    {
      if (s.matches(item))
      {
        return true;
      }
    }
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.schema.Schema#toString()
   */
  @Override
  public String toString()
  {
    String str = "(";
    String sep = "";
    for (Schema s = schemaList; s != null; s = s.nextSchema)
    {
      str += sep;
      str += s.toString();
      sep = " | ";
    }
    str += ")";
    return str;
  }
}

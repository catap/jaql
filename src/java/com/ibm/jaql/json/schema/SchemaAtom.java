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
import com.ibm.jaql.json.type.JString;

/**
 * 
 */
public class SchemaAtom extends Schema // TODO: used classes for each type? eg, max string len
{
  private static JString tempName = new JString();

  public Item.Type       type;

  /**
   * @param type
   */
  public SchemaAtom(Item.Type type)
  {
    this.type = type;
  }

  /**
   * @param typeName
   */
  public SchemaAtom(String typeName)
  {
    type = Item.Type.getType(typeName);
    if (type == null || type == Item.Type.UNKNOWN)
    {
      throw new RuntimeException("unknown atom type: " + typeName);
    }
  }

  /**
   * @param in
   * @throws IOException
   */
  public SchemaAtom(DataInput in) throws IOException
  {
    synchronized (tempName)
    {
      tempName.readFields(in);
      type = Item.Type.getType(tempName);
      if (type == null || type == Item.Type.UNKNOWN)
      {
        throw new IOException("unknown atom type: " + tempName);
      }
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
    out.writeByte(ATOM_TYPE);
    type.nameValue.write(out);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.schema.Schema#matches(com.ibm.jaql.json.type.Item)
   */
  @Override
  public boolean matches(Item item)
  {
    return item.getEncoding().type == type;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.schema.Schema#toString()
   */
  @Override
  public String toString()
  {
    return type.name;
  }
}

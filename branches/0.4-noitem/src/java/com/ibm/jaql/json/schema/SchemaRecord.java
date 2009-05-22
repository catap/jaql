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

import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.BaseUtil;

/** Schema that matches a record, i.e., an ordered list of fields.
 * 
 *  @see SchemaField
 */
public class SchemaRecord extends Schema
{
  protected SchemaField fullName;
  // protected SchemaField prefix;
  protected Schema      rest; // Null if record is "closed"

  /**
   * 
   */
  public SchemaRecord()
  {
  }

  /**
   * @param in
   * @throws IOException
   */
  public SchemaRecord(DataInput in) throws IOException
  {
    int n = BaseUtil.readVUInt(in);
    for(int i = 0 ; i < n ; i++)
    {
      SchemaField fs = new SchemaField(in);
      addField(fs);
    }
    rest = Schema.read(in);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.schema.Schema#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException
  {
    out.writeByte(RECORD_TYPE);
    int n = 0;
    for (SchemaField fs = fullName; fs != null; fs = fs.nextField)
    {
      n++;
    }
    BaseUtil.writeVUInt(out, n);    
    for (SchemaField fs = fullName; fs != null; fs = fs.nextField)
    {
      fs.write(out);
    }
    if( rest == null )
    {
      out.writeByte(UNKNOWN_TYPE);
    }
    else
    {
      rest.write(out);
    }
  }

  /**
   * @param fs
   */
  public void addField(SchemaField fs)
  {
    // TODO: keep in an array so we can do binary search.
    // add in sorted order
    SchemaField prev;
    SchemaField cur;
    for (prev = null, cur = fullName; cur != null; prev = cur, cur = cur.nextField)
    {
      int c = cur.name.compareTo(fs.name);
      if (c > 0)
      {
        break;
      }
      else if (c == 0)
      {
        throw new RuntimeException("duplicate field name not allowed: "
            + cur.name);
      }
    }
    fs.nextField = cur;
    if (prev == null)
    {
      fullName = fs;
    }
    else
    {
      prev.nextField = fs;
    }
  }
  
  /**
   * Set the schema of all fields that are not among the named fields.
   * Set to null to disallow other fields (close the record).
   * 
   * @param rest
   */
  public void setRest(Schema rest)
  {
    this.rest = rest;
  }

  /**
   * @return The schema for unnamed fields.
   */
  public Schema getRest()
  {
    return rest;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.schema.Schema#matches(com.ibm.jaql.json.type.Item)
   */
  @Override
  public boolean matches(JsonValue value) throws Exception
  {
    if (!(value instanceof JsonRecord))
    {
      return false;
    }
    JsonRecord rec = (JsonRecord) value;

    SchemaField fs = fullName;
    int n = rec.arity();
    for (int i = 0; i < n; i++)
    {
      JsonString fieldName = rec.getName(i);
      JsonValue fieldValue = rec.getValue(i);

      int c = 1;
      while (fs != null && (c = fs.name.compareTo(fieldName)) < 0)
      {
        if (!fs.optional)
        {
          return false;
        }
        fs = fs.nextField;
        c = 1;
      }

      if (c == 0)
      {
        if (!fs.schema.matches(fieldValue))
        {
          return false;
        }
        fs = fs.nextField;
      }
      else
      // fs == null || fs.name > fieldName
      {
        if( rest == null || ! rest.matches(fieldValue) )
        {
          return false;
        }
      }
    }

    while (fs != null)
    {
      if (!fs.optional)
      {
        return false;
      }
      fs = fs.nextField;
    }

    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.schema.Schema#toString()
   */
  @Override
  public String toString()
  {
    String str = "{";
    String sep = " ";
    for (SchemaField fs = fullName; fs != null; fs = fs.nextField)
    {
      str += sep + fs.toString();
      sep = ", ";
    }
    if( rest != null )
    {
      str += sep + "*: " + rest.toString();
    }
    str += " }";
    return str;
  }
}

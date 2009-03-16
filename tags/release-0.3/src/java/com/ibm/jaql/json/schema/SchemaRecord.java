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
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;

/**
 * 
 */
public class SchemaRecord extends Schema
{
  protected SchemaField fullName;
  protected SchemaField prefix;

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
    while (in.readByte() != 0)
    {
      SchemaField fs = new SchemaField(in);
      addField(fs);
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
    out.writeByte(RECORD_TYPE);
    for (SchemaField fs = fullName; fs != null; fs = fs.nextField)
    {
      out.writeByte(1);
      fs.write(out);
    }
    for (SchemaField fs = prefix; fs != null; fs = fs.nextField)
    {
      out.writeByte(1);
      fs.write(out);
    }
    out.writeByte(0);
  }

  /**
   * @param fs
   */
  public void addField(SchemaField fs)
  {
    if (fs.wildcard)
    {
      fs.nextField = prefix;
      prefix = fs;
    }
    else
    {
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
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.schema.Schema#matches(com.ibm.jaql.json.type.Item)
   */
  @Override
  public boolean matches(Item item) throws Exception
  {
    if (!(item.get() instanceof JRecord))
    {
      return false;
    }
    JRecord rec = (JRecord) item.get();

    SchemaField fs = fullName;
    int n = rec.arity();
    for (int i = 0; i < n; i++)
    {
      JString recName = rec.getName(i);
      Item recValue = rec.getValue(i);

      int c = 1;
      while (fs != null && (c = fs.name.compareTo(recName)) < 0)
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
        if (!fs.schema.matches(recValue))
        {
          return false;
        }
        fs = fs.nextField;
      }
      else
      // fs == null || fs.name > rec.name
      {
        // At least one wildcard schema must match, AND
        // Every required wildcard schema that prefix matches must match the value.
        boolean matched = false;
        for (SchemaField ps = prefix; ps != null; ps = ps.nextField)
        {
          if (recName.startsWith(ps.name))
          {
            if (!ps.schema.matches(recValue))
            {
              return false;
            }
            matched = true;
          }
        }
        if (!matched)
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
    for (SchemaField fs = prefix; fs != null; fs = fs.nextField)
    {
      str += sep;
      str += sep + fs.toString();
      sep = ",";
    }
    return str;
  }
}

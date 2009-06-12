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
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.util.BaseUtil;

/**
 * 
 */
public class SchemaArray extends Schema
{
  public final static long UNLIMITED = Long.MAX_VALUE;

  // If minCount == maxCount then this is a fixed-length array (head length + rest count)
  // If head == rest == null, then this is an empty array (ie, minCount == maxCount == 0)

  public Schema            head;                      // list of schemas for the first n elements (or null)
  public Schema            rest;                      // single schema for all remaining items (or null iff maxCount == 0)
  public long              minCount;                  // min number of items matching rest (>= 0)
  public long              maxCount;                  // max number of items matching rest, or unlimited (>= minCount)

  /**
   * 
   */
  public SchemaArray()
  {
  }

  /**
   * @param in
   * @throws IOException
   */
  public SchemaArray(DataInput in) throws IOException
  {
    head = Schema.read(in);
    if (head != null)
    {
      Schema prev = head;
      Schema s;
      do
      {
        s = Schema.read(in);
        prev.nextSchema = s;
      } while (s != null);
    }
    minCount = BaseUtil.readVULong(in);
    maxCount = BaseUtil.readVULong(in);
    if (maxCount > 0)
    {
      rest = Schema.read(in);
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
    out.writeByte(ARRAY_TYPE);
    for (Schema s = head; s != null; s = s.nextSchema)
    {
      s.write(out);
    }
    out.writeByte(UNKNOWN_TYPE);
    BaseUtil.writeVULong(out, minCount);
    BaseUtil.writeVULong(out, maxCount);
    if (rest != null)
    {
      assert maxCount > 0;
      rest.write(out);
    }
  }

  /**
   * typeList can be null
   * 
   * @param typeList
   */
  public void noRepeat(Schema typeList)
  {
    head = typeList;
    rest = null;
    minCount = maxCount = 0;
  }

  /**
   * typeList cannot be null. The last item is the repeating tail.
   * 
   * @param typeList
   * @param lo
   * @param hi
   */
  public void setRepeat(Schema typeList, long lo, long hi)
  {
    if (lo > hi || hi < 0)
    {
      throw new RuntimeException("array repeatition out of bounds: " + lo + ","
          + hi);
    }
    minCount = lo;
    maxCount = hi;
    // Split the head and tail
    if (typeList.nextSchema == null)
    {
      head = null;
      rest = typeList;
    }
    else
    // remove the last item in the list
    {
      head = typeList;
      Schema prev;
      do
      {
        prev = typeList;
        typeList = typeList.nextSchema;
      } while (typeList.nextSchema != null);
      prev.nextSchema = null;
      rest = typeList;
    }
    if (hi == 0)
    {
      rest = null;
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
    if (!(item.get() instanceof JArray))
    {
      return false;
    }
    JArray arr = (JArray) item.get();
    Iter iter = arr.iter();
    for (Schema s = head; s != null; s = s.nextSchema)
    {
      item = iter.next();
      if (item == null || !s.matches(item))
      {
        return false;
      }
    }
    if (rest == null)
    {
      if (iter.next() != null)
      {
        return false;
      }
      return true;
    }
    // rest != null
    long n = 0;
    while ((item = iter.next()) != null)
    {
      n++;
      if (n > maxCount || !rest.matches(item))
      {
        return false;
      }
    }
    return n >= minCount;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.schema.Schema#toString()
   */
  @Override
  public String toString()
  {
    String str = "[ ";
    String sep = "";
    for (Schema s = head; s != null; s = s.nextSchema)
    {
      str += sep;
      str += s.toString();
      sep = ", ";
    }
    if (rest != null)
    {
      str += sep;
      str += rest.toString();
      if (maxCount > 0)
      {
        str += " <" + minCount + ",";
        if (maxCount < UNLIMITED)
        {
          str += maxCount;
        }
        else
        {
          str += "*";
        }
        str += ">";
      }
    }
    str += " ]";
    return str;
  }

}

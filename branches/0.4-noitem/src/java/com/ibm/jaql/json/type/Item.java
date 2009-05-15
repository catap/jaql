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
package com.ibm.jaql.json.type;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;

import com.ibm.jaql.io.serialization.FullSerializer;

/** Encapsulates an arbitrary, single JSON value (instance of {@link JsonValue}). */
@Deprecated
public final class Item implements Comparable<Item> // , Cloneable
{
  public static final Item   NIL      = new Item();
  public static final Item[] NO_ITEMS = new Item[0];

  private JsonEncoding           encoding;
  private JsonValue             value;
  private JsonValue             cache;                 // cache is the most recent non-null value (value == cache when value != null)

  /**
   * @param x
   * @param y
   * @return
   */
  public static int typeCompare(Item x, Item y)
  {
    return x.encoding.type.compareTo(y.encoding.type);
  }

  /**
   * 
   */
  public Item()
  {
    this.encoding = JsonEncoding.NULL;
  }

  /** Does not copy.
   * @param v
   */
  public Item(JsonValue v)
  {
    set(v);
  }

  /**
   * @return
   */
  public boolean isNull()
  {
    return (value == null);
  }

  /**
   * @return
   */
  public JsonEncoding getEncoding()
  {
    return encoding;
  }

  /**
   * @return
   */
  public JsonType getType()
  {
    return encoding.getType();
  }

  /**
   * @return
   */
  public JsonValue getNonNull()
  {
    if (value == null)
    {
      throw new NullPointerException("value must be non-null");
    }
    return value;
  }

  /** Returns the JValue represented by this item
   * @return
   */
  public JsonValue get() // TODO: delete me
  {
    return value;
  }

  /**
   * @return
   */
  public JsonValue restoreCache()
  {
    value = cache;
    return value;
  }

  /**
   * Set the value of this item to <code>v</code> without copying.
   * 
   * @param v a value
   */
  public void set(JsonValue v)
  {
    // TODO: cache old value.  save value when type is null?
    if (v == null) {
      encoding = JsonEncoding.NULL;
      value = null;      
    } else {
      encoding = v.getEncoding();
      cache = value = v;
    }
  }

  /** Set the value of this item to a copy of <code>v</code>. 
   * 
   * @param v a value
   * @throws Exception
   */
  public void setCopy(JsonValue v) throws Exception 
  {
    if (v == null) {
      encoding = JsonEncoding.NULL;
      value = null;
    } else {
      if (encoding != v.getEncoding() || value==v)
      {
        encoding = v.getEncoding();
        cache = value = encoding.newInstance();
      }
      value.setCopy(v);
    }
  }

  /** Copy the content of <code>item</code> into this object. 
   * 
   * @param item an item
   * @throws Exception
   */
  public void setCopy(Item item) throws Exception
  {
    if (item == null) {
      encoding = JsonEncoding.NULL;
      value = null;
      return;
    }
    setCopy(item.get());
  }

  //still used by some input formats
  public void readFields(DataInput in) throws IOException
  {
    cache = value = FullSerializer.getDefault().read(in, value);
    if (value != null)
    {
      encoding = value.getEncoding();
    } else {
      encoding = JsonEncoding.NULL;
    }
  }

  //still used by some output formats
  public void write(DataOutput out) throws IOException
  {
    FullSerializer.getDefault().write(out, value);
  }
  

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#equals(java.lang.Object)
   */
  public boolean equals(Object x)
  {
    return this.compareTo((Item)x) == 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  public int compareTo(Item x)
  {
    int cmp = typeCompare(this, x);
    if (cmp != 0)
    {
      return cmp;
    }
    if (value == null)
    {
      assert encoding == JsonEncoding.NULL;
      return 0;
    }
    return value.compareTo(x.value);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode()
  {
    if (value == null)
    {
      assert encoding == JsonEncoding.NULL;
      return 0;
    }
    return value.hashCode();
  }

  /**
   * @return
   */
  public long longHashCode()
  {
    if (value == null)
    {
      assert encoding == JsonEncoding.NULL;
      return 0;
    }
    return value.longHashCode();
  }

  /**
   * @return
   */
  public String toJSON()
  {
    if (value == null)
    {
      return "null";
    }
    else
    {
      return value.toJson();
    }
  }

  /**
   * @param out
   * @throws Exception
   */
  public void print(PrintStream out) throws Exception
  {
    if (value == null)
    {
      out.print("null");
    }
    else
    {
      value.print(out);
    }
  }

  /**
   * @param out
   * @param indent
   * @throws Exception
   */
  public void print(PrintStream out, int indent) throws Exception
  {
    if (value == null)
    {
      out.print("null");
    }
    else
    {
      value.print(out, indent);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    try
    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream out = new PrintStream(baos);
      this.print(out, 0);
      out.flush();
      return baos.toString();
    }
    catch (Exception e)
    {
      return "exception()";
    }
  }

  /**
   * @return
   */
  final public boolean isAtom()
  {
    return !(value instanceof JsonRecord || value instanceof JsonArray);
  }
  
  /** Reset the content of this item. After resetting, the state of this item is identical 
   * to the state of a newly created item. All internal values and cache elements referenced by 
   * this item are not touched; but the reseted item does not reference them anymore.
   */
  public void reset() {
    encoding = JsonEncoding.NULL;
    value = null;
    cache = null;
  }
}

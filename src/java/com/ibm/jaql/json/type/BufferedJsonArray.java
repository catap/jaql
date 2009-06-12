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

import com.ibm.jaql.json.util.JsonIterator;

/*
 * This is a fixed-sized array (i.e., a tuple). It is used when the length of
 * the array is known at in advance and does not vary. It does NOT copy the
 * items placed inside of it.
 * 
 * This array class does NOT copy items added to it; the items are referenced.
 * Therefore, it is crucial that the items remain valid for the lifetime of this
 * array.
 * 
 */
public final class BufferedJsonArray extends JsonArray
{
  public final static JsonValue[] NO_VALUES = new JsonValue[0];

  protected JsonValue[] values;
  protected int    count;

  /**
   * @param values
   */
  public BufferedJsonArray(JsonValue[] values)
  {
    this.values = values;
    this.count = values.length;
  }

  /**
   * @param size
   */
  public BufferedJsonArray(int size)
  {
    this(new JsonValue[size]);
  }

  /**
   * 
   */
  public BufferedJsonArray()
  {
    this(NO_VALUES);
  }

  /**
   * 
   */
  public void clear()
  {
    count = 0;
  }

  /**
   * @param size
   */
  public final void ensureCapacity(int size)
  {
    if (size > values.length)
    {
      JsonValue[] newValues = new JsonValue[size];
      System.arraycopy(this.values, 0, newValues, 0, values.length);
      this.values = newValues;
    }
  }

  /**
   * @param size
   */
  public void resize(int size)
  {
    ensureCapacity(size);
    this.count = size;
  }

  /**
   * @param size
   */
  public void resize(long size)
  {
    resize((int) size);
  }

  /**
   * Same as count() except that it returns an int.
   * 
   * @return
   */
  public final int size()
  {
    return count;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#count()
   */
  public final long count()
  {
    return count;
  }

  /**
   * @param i
   * @return
   */
  public JsonValue get(int i)
  {
    assert i < count;
    return values[i];
  }

  /**
   * @param i
   * @param item
   */
  public void set(int i, JsonValue value)
  {
    assert i < count;
    values[i] = value;
  }

  /**
   * @param item
   */
  public void add(JsonValue value)
  {
    ensureCapacity(count + 1);
    values[count] = value;
    count++;
  }

    /**
   * @param x
   * @return
   */
  public final int compareTo(BufferedJsonArray x)
  {
    int n = count;
    if (x.count < n)
    {
      n = x.count;
    }
    for (int i = 0; i < n; i++)
    {
      int c = values[i].compareTo(x.values[i]);
      if (c != 0)
      {
        return c;
      }
    }
    if (count == x.count)
    {
      return 0;
    }
    else if (count < x.count)
    {
      return +1;
    }
    else
    {
      return -1;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(Object x)
  {
    if (x instanceof BufferedJsonArray)
    {
      return compareTo((BufferedJsonArray) x);
    }
    return super.compareTo(x);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#copy(com.ibm.jaql.json.type.JValue)
   */
  @Override
  public void setCopy(JsonValue value) throws Exception
  {
    BufferedJsonArray arr = (BufferedJsonArray) value;
    int n = arr.count;
    resize(n);
    for (int i = 0; i < n; i++)
    {
      values[i] = arr.values[i].getCopy(values[i]);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#getTuple(com.ibm.jaql.json.type.JValue[])
   */
  @Override
  public void getValues(JsonValue[] values) throws Exception
  {
    assert values.length == count;
    System.arraycopy(this.values, 0, values, 0, count);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#longHashCode()
   */
  @Override
  public long longHashCode()
  {
    long h = initLongHash();
    for (int i = 0; i < count; i++)
    {
      h = longHashValue(h, values[i]);
    }
    return h;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#iter()
   */
  @Override
  public JsonIterator iter() throws Exception
  {
    if (count == 0)
    {
      return JsonIterator.EMPTY;
    }
    return new JsonIterator() {
      int i = 0;

      @Override
      public boolean moveNext() throws Exception
      {
        if (i < count)
        {
          currentValue = values[i++];
          return true;
        }
        return false;
      }
    };
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#nth(long)
   */
  @Override
  public JsonValue nth(long n) throws Exception
  {
    if (n >= 0 && n < count)
    {
      return values[(int) n];
    }
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.ARRAY_FIXED;
  }
}

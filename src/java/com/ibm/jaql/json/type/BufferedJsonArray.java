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

/** An in-memory array.
 * 
 * Although this class provides basic operations for mutation, it is meant to be a fixed-sized 
 * array (i.e., a tuple). It should be used when the length of the array is known at in advance 
 * and does not vary. 
 * 
 * This array class does NOT copy the values added to it; they are referenced. Therefore, it is 
 * crucial that the values remain valid for the lifetime of this array. 
 */
public final class BufferedJsonArray extends JsonArray
{
  private final static JsonValue[] NO_VALUES = new JsonValue[0];
  
  /** number of values in the array */
  private int count;

  /** buffer that stores the content of the array */
  private JsonValue[] values;
  
  
  // -- construction ------------------------------------------------------------------------------
  
  /** Construct a new in-memory array containing the specified values. */
  public BufferedJsonArray(JsonValue[] values)
  {
    this.values = values;
    this.count = values.length;
  }

  /** Construct a new in-memory array of the specified size. The array is initially filled with
   * <code>null</code> values. */
  public BufferedJsonArray(int size)
  {
    this(new JsonValue[size]);
  }

  /** Construct an empty in-memory array */
  public BufferedJsonArray()
  {
    this(NO_VALUES);
  }

  
  // -- mutation ----------------------------------------------------------------------------------
  
  /** Appends the specified value to this array */
  public void add(JsonValue value)
  {
    ensureCapacity(count + 1);
    values[count] = value;
    count++;
  }
  
  /** Sets the i-th element of this array to the specified value without boundary checking. 
   * Correctly sets the desired element whenever 0&le;<code>i</code>&le;<code>count()</code>. 
   * Otherwise, the behaviour is unspecified. */
  public void set(int i, JsonValue value)
  {
    assert i < count;
    values[i] = value;
  }

  
  /* @see com.ibm.jaql.json.type.JsonValue#getCopy(com.ibm.jaql.json.type.JsonValue) */
  @Override
  public BufferedJsonArray getCopy(JsonValue target) throws Exception
  {
    if (target == this) target = null;
    
    BufferedJsonArray t;
    if (target instanceof BufferedJsonArray)
    {
      t = (BufferedJsonArray)target;
      t.resize(count);
    }
    else
    {
      t = new BufferedJsonArray(count);
    }
    
    for (int i = 0; i < count; i++)
    {
      JsonValue v = values[i]; 
      t.values[i] = v==null ? null : v.getCopy(values[i]);
    }
    return t;
  }
  
  /** Ensures that the array can store up to <code>capacity</code> elements but does not change
   * the actual size or content of the array. */
  public final void ensureCapacity(int capacity)
  {
    if (capacity > values.length)
    {
      JsonValue[] newValues = new JsonValue[capacity];
      System.arraycopy(this.values, 0, newValues, 0, values.length);
      this.values = newValues;
    }
  }

  /** Resizes this array to the specified size. If <code>newSize<count()</code>, the tail of
   * the array will be truncated. If <code>newSize>count()</code>, new elements are appended to
   * the end of the array; their content is undefined. */
  public void resize(int newSize)
  {
    // NOTE: shrinking an array and then growing it again makes the values that have been 
    // truncated reappear: e.g., [1,2,3] --resize(2)--> [1,2] --resize(3)--> [1,2,3]  
    ensureCapacity(newSize);
    this.count = newSize;
  }

  /** Resizes this array to the specified size when interpreted as an <code>int</code>. See
   * {@link #resize(int)}. */
  public void resize(long newSize)
  {
    resize((int) newSize);
  }

  /** Clears this array, i.e., sets its size to zero. */
  public void clear()
  {
    count = 0;
  }
  
  
  // -- getters -----------------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonArray#count() */
  @Override
  public final long count()
  {
    return count;
  }

  /** Returns the size of this array. Same as {@link #count()} except that it returns an int. */
  public final int size()
  {
    return count;
  }

  /* @see com.ibm.jaql.json.type.JArray#iter() */
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

  /* @see com.ibm.jaql.json.type.JArray#nth(long) */
  @Override
  public JsonValue nth(long n) throws Exception
  {
    if (n >= 0 && n < count)
    {
      return values[(int) n];
    }
    return null;
  }

  /** Returns the i-th element of this array without boundary checking. Produces the desired
   * element whenever 0&le;<code>i</code>&le;<code>count()</code>. Otherwise, the behaviour is
   * unspecified. */
  public JsonValue get(int i)
  {
    assert i < count;
    return values[i];
  }
  
  /* @see com.ibm.jaql.json.type.JsonArray#getAll(com.ibm.jaql.json.type.JsonValue[]) */
  @Override
  public void getAll(JsonValue[] target) throws Exception
  {
    assert target.length == count;
    System.arraycopy(this.values, 0, target, 0, count);
  }



  // -- comparison/hashing ------------------------------------------------------------------------
  
  /** Compares this array with the specified array (deep). */
  public int compareTo(BufferedJsonArray x)
  {
    int n = count;
    if (x.count < n)
    {
      n = x.count;
    }
    for (int i = 0; i < n; i++)
    {
      int c = JsonUtil.compare(values[i], x.values[i]);
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

  /* @see com.ibm.jaql.json.type.JsonArray#compareTo(java.lang.Object) */
  @Override
  public int compareTo(Object x)
  {
    if (x instanceof BufferedJsonArray)
    {
      return compareTo((BufferedJsonArray) x);
    }
    return super.compareTo(x);
  }
  

  // -- misc --------------------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonValue#getEncoding() */
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.ARRAY_FIXED;
  }
}

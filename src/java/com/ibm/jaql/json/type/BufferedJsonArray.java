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
  protected static final int MIN_CAPACITY = 8; // when non-empty
  private final static JsonValue[] NO_VALUES = new JsonValue[0];
  
  /** number of values in the array */
  private int size;

  /** buffer that stores the content of the array */
  private JsonValue[] values;
  
  
  // -- construction ------------------------------------------------------------------------------

  /** Construct a new buffered array containing the specified values. If <code>copy</code> 
   * is set to false, the constructed array will be backed by <code>values</code>. 
   * Otherwise, a deep copy of <code>values</code> will be used. */
  public BufferedJsonArray(JsonValue[] values, boolean copy)
  {
    this(values, values.length, copy);
  }

  /** Construct a new buffered array containing the specified values. If <code>copy</code> 
   * is set to false, the constructed array will be backed by <code>values</code>. 
   * Otherwise, a deep copy of <code>values</code> will be used. */
  public BufferedJsonArray(JsonValue[] values, int size, boolean copy)
  {
    if (copy)
    {
      setCopy(values, size);
    }
    else
    {
      set(values, size);
    }
  }

  /** Construct a new in-memory array of the specified size. The array is initially filled with
   * <code>null</code> values. */
  public BufferedJsonArray(int size)
  {
    this(new JsonValue[size], false);
  }

  /** Construct an empty in-memory array */
  public BufferedJsonArray()
  {
    this(NO_VALUES, false);
  }

  
  // -- getters -----------------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonArray#count() */
  @Override
  public final long count()
  {
    return size;
  }

  /** Returns the size of this array. Same as {@link #count()} except that it returns an int. */
  public final int size()
  {
    return size;
  }

  /* @see com.ibm.jaql.json.type.JArray#iter() */
  @Override
  public JsonIterator iter() throws Exception
  {
    if (size == 0)
    {
      return JsonIterator.EMPTY;
    }
    return new JsonIterator() {
      int i = 0;

      @Override
      public boolean moveNext() throws Exception
      {
        if (i < size)
        {
          currentValue = values[i++];
          return true;
        }
        return false;
      }
    };
  }

  /* @see com.ibm.jaql.json.type.JsonArray#get(long) */
  @Override
  public JsonValue get(long n) throws Exception
  {
    if (n >= 0 && n < size)
    {
      return values[(int) n];
    }
    return null;
  }

  /** Returns the i-th element of this array without boundary checking. Produces the desired
   * element whenever 0&le;<code>i</code>&le;<code>count()</code>. Otherwise, the behaviour is
   * unspecified. */
  public JsonValue getUnchecked(int i)
  {
    assert i < size;
    return values[i];
  }
  
  /* @see com.ibm.jaql.json.type.JsonArray#getAll(com.ibm.jaql.json.type.JsonValue[]) */
  @Override
  public void getAll(JsonValue[] target) throws Exception
  {
    assert target.length == size;
    System.arraycopy(this.values, 0, target, 0, size);
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
      t.resize(size);
    }
    else
    {
      t = new BufferedJsonArray(size);
    }
    
    for (int i = 0; i < size; i++)
    {
      JsonValue v = values[i]; 
      t.values[i] = v==null ? null : v.getCopy(values[i]);
    }
    return t;
  }

  @Override
  public JsonArray getImmutableCopy() throws Exception
  {
    // FIXME: copy is not immutable
    return getCopy(null);
  }
  
  // -- mutation ----------------------------------------------------------------------------------
  
  /** Sets this JSON array to the first <code>size</code> values in the given array. The array is not 
   * copied but directly used as internal buffer. */
  public void set(JsonValue values[], int size)
  {
    if (size > values.length) throw new IllegalArgumentException();
    this.values = values;
    this.size = size;
  }
  
  /** Sets this JSON array to a (deep) copy of the first <code>size</code> values in the given 
   * array. */
  public void setCopy(JsonValue values[], int size)
  {
    if (size > values.length) throw new IllegalArgumentException();
    resize(size);
    for (int i=0; i<size; i++)
    {
      this.values[i] = JsonUtil.getCopyUnchecked(values[i], this.values[i]);
    }
  }
  
  /** Appends the specified value to this array */
  public void add(JsonValue value)
  {
    ensureCapacity(size + 1);
    values[size] = value;
    size++;
  }
  
  /** Appends a copy of the specified value to this array */
  public void addCopy(JsonValue value)
  {
    ensureCapacity(size + 1);
    values[size] = JsonUtil.getCopyUnchecked(value, values[size]);
    size++;
  }
  
  /** Sets the i-th element of this array to the specified value without boundary checking. 
   * Correctly sets the desired element whenever 0&le;<code>i</code>&le;<code>count()</code>. 
   * Otherwise, the behaviour is unspecified. */
  public void set(int i, JsonValue value)
  {
    assert i < size;
    values[i] = value;
  }

  /** Sets the i-th element of this array to a copy of the specified value without boundary checking. 
   * Correctly sets the desired element whenever 0&le;<code>i</code>&le;<code>count()</code>. 
   * Otherwise, the behaviour is unspecified. */
  public void setCopy(int i, JsonValue value)
  {
    assert i < size;
    values[i] = JsonUtil.getCopyUnchecked(value, values[i]);
  }
  
  /** Ensures that the array can store up to <code>capacity</code> elements but does not change
   * the actual size or content of the array. */
  public final void ensureCapacity(int capacity)
  {
    if (values.length < capacity)
    {
      int newCapacity = Math.max(MIN_CAPACITY, values.length);
      while (newCapacity < capacity) newCapacity *= 2;

      JsonValue[] newValues = new JsonValue[newCapacity];
      System.arraycopy(this.values, 0, newValues, 0, values.length);
      this.values = newValues;
    }
  }

  /** Resizes this array to the specified size. If <code>size<count()</code>, the tail of
   * the array will be truncated. If <code>size>count()</code>, new elements are appended to
   * the end of the array; their content is undefined. */
  public void resize(int size)
  {
    // NOTE: shrinking an array and then growing it again makes the values that have been 
    // truncated reappear: e.g., [1,2,3] --resize(2)--> [1,2] --resize(3)--> [1,2,3]  
    ensureCapacity(size);
    this.size = size;
  }

  /** Clears this array, i.e., sets its size to zero. */
  public void clear()
  {
    size = 0;
  }
  

  // -- comparison/hashing ------------------------------------------------------------------------
  
  /** Compares this array with the specified array (deep). */
  public int compareTo(BufferedJsonArray x)
  {
    int n = size;
    if (x.size < n)
    {
      n = x.size;
    }
    for (int i = 0; i < n; i++)
    {
      int c = JsonUtil.compare(values[i], x.values[i]);
      if (c != 0)
      {
        return c;
      }
    }
    if (size == x.size)
    {
      return 0;
    }
    else if (size < x.size)
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
  
  /* @see com.ibm.jaql.json.type.JsonValue#getEncoding() */
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.ARRAY_BUFFERED;
  }
}

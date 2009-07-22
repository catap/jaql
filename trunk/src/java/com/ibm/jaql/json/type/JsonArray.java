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

import java.lang.reflect.UndeclaredThrowableException;

import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.json.util.JsonUtil;
import com.ibm.jaql.util.BaseUtil;

/** A JSON array. */
public abstract class JsonArray extends JsonValue implements Iterable<JsonValue>
{
  public final static JsonArray EMPTY 
      = new BufferedJsonArray(new JsonValue[0]); // TODO: should be immutable

  
  // -- abstract methods --------------------------------------------------------------------------
  
  /** Returns the number of values stored in this array.
   * 
   * @return the number of values stored in this array
   */
  public abstract long count();

  /** Returns an {@link JsonIterator} over the elements in this array.
   *   
   * @return an iterator over the elements in this array
   * @throws Exception
   */
  public abstract JsonIterator iter() throws Exception;

  /** Returns the value at position <code>n</code> or <code>null<code> if there is no such value.
   * This method is expensive in some implementations; prefer {@link #iter()} if possible. 
   * 
   * @param n a position (0-based)
   * @return the value at position <code>n</code> or <code>null<code>
   * @throws Exception
   */
  public abstract JsonValue nth(long n) throws Exception;

  /** Fills the specified array with the elements of this array (without necessarily copying the 
   * elements themselves). The length of <code>target</code> has to be identical to the length of 
   * this array as produced by {@link #count()}.
   * 
   * @param target an array
   * @throws Exception
   */
  public abstract void getAll(JsonValue[] target) throws Exception;


  // -- business methods --------------------------------------------------------------------------
  
  /** Checks whether this array contains any elements. */
  public boolean isEmpty()
  {
    return count() == 0;
  }

  /** Returns an iterator over this array. This is a convenience method that allows 
   * using <code>JsonArray</code>s in <code>foreach</code> statements. It as implemented by a 
   * call to {@link #iter()}; all exceptions are rethrown as runtime exceptions. */ 
  @Override
  public JsonIterator iterator() {
    try 
    {
      return iter();
    } 
    catch (Exception e)
    {
      throw new UndeclaredThrowableException(e);
    }    
  }

  /* @see com.ibm.jaql.json.type.JValue#equals(java.lang.Object) */
  @Override
  public final boolean equals(Object x)
  {
    return this.compareTo(x) == 0;
  }

  /* @see com.ibm.jaql.json.type.JValue#compareTo(java.lang.Object) */
  @Override
  public int compareTo(Object x)
  {
    JsonArray that = (JsonArray) x;
    try
    {
      return JsonUtil.deepCompare(this.iter(), that.iter());
    }
    catch (Exception ex)
    {
      throw new UndeclaredThrowableException(ex);
    }
  }

  
  // -- hashing -----------------------------------------------------------------------------------

  /* @see java.lang.Object#hashCode() */
  @Override
  public int hashCode()
  {
    try
    {
      int h = initHash();
      for (JsonValue value : iter()) {
        h = hashValue(h, value);
      }
      return h;
    }
    catch (Exception e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }

  protected final static int initHash()
  {
    return BaseUtil.GOLDEN_RATIO_32;
  }

  protected final static int hashValue(int h, JsonValue value)
  {
    int v = com.ibm.jaql.json.type.JsonUtil.hashCode(value);
    return (h ^ v) * BaseUtil.GOLDEN_RATIO_32;
  }
  
  /* @see com.ibm.jaql.json.type.JValue#longHashCode() */
  @Override
  public long longHashCode()
  {
    try
    {
      long h = initLongHash();
      for (JsonValue value : iter()) {
        h = longHashValue(h, value);
      }
      return h;
    }
    catch (Exception e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }

  protected final static long initLongHash()
  {
    return BaseUtil.GOLDEN_RATIO_64;
  }

  protected final static long longHashValue(long h, JsonValue value)
  {
    long v = com.ibm.jaql.json.type.JsonUtil.longHashCode(value);
    return (h ^ v) * BaseUtil.GOLDEN_RATIO_64;
  }
 
}

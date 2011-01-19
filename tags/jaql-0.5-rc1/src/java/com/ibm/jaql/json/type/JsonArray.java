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
import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;

import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.json.util.JsonUtil;
import com.ibm.jaql.util.BaseUtil;

/** A JSON array. Compatibility with the Java Collection Framework is provided via the 
 * {@link #asList()} method. */ 
public abstract class JsonArray extends JsonValue 
implements Iterable<JsonValue>
{
  public final static JsonArray EMPTY 
      = new BufferedJsonArray(new JsonValue[0], false); // TODO: should be immutable

  
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
  public abstract JsonValue get(long n) throws Exception;

  /** Fills the specified array with the elements of this array (without necessarily copying the 
   * elements themselves). The length of <code>target</code> has to be identical to the length of 
   * this array as produced by {@link #count()}.
   * 
   * @param target an array
   * @throws Exception
   */
  public abstract void getAll(JsonValue[] target) throws Exception;


  // -- default implementation --------------------------------------------------------------------
  
  /** Checks whether this array is empty */
  public boolean isEmpty()
  {
    return count() == 0;
  }
  
  @Override
  public Iterator<JsonValue> iterator() {
    try 
    {
      return iter();
    } 
    catch (Exception e)
    {
      throw new UndeclaredThrowableException(e);
    }    
  }

  
  // -- comparison and hashing --------------------------------------------------------------------

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

  /* @see com.ibm.jaql.json.type.JValue#longHashCode() */
  @Override
  public long longHashCode()
  {
    try
    {
      long h = BaseUtil.GOLDEN_RATIO_64;
      for (JsonValue value : iter()) {
        long v = com.ibm.jaql.json.type.JsonUtil.longHashCode(value); 
        h = (h ^ v) * BaseUtil.GOLDEN_RATIO_64;
      }
      return h;
    }
    catch (Exception e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }

  // -- List compatibility ------------------------------------------------------------------------
  
  /** Returns an unmodifiable list view of this JSON array */
  public List<JsonValue> asList()
  {
    return new JsonArrayAsList();
  }
  
  /** A list view of this JSON array */
  private class JsonArrayAsList extends AbstractList<JsonValue>
  {
    @Override
    public JsonValue get(int index)
    {
      try 
      {
        return JsonArray.this.get(index);
      }
      catch (Exception e)
      {
        throw new UndeclaredThrowableException(e);
      }
      
    }

    @Override
    public int size()
    {
      return (int)JsonArray.this.count();
    }
    
    @Override 
    public Iterator<JsonValue> iterator()
    {
      return JsonArray.this.iterator();
    }
  }
}

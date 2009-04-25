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
import java.io.PrintStream;
import java.lang.reflect.UndeclaredThrowableException;

import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.json.util.IterJIterator;
import com.ibm.jaql.json.util.JIterator;
import com.ibm.jaql.json.util.JsonUtil;
import com.ibm.jaql.util.BaseUtil;

/** A JSON array.
 * 
 */
public abstract class JArray extends JValue
{
  public final static FixedJArray EMPTY      = FixedJArray.getEmpty();
  public final static Item        EMPTY_ITEM = new Item(EMPTY);

  
  // -- abstract methods --------------------------------------------------------------------------
  
  /** Returns the number of elements stored in this array.
   * 
   * @return the number of elements stored in this array
   */
  public abstract long count();

  /** Returns an <code>Iter</code> over the elements in this array.
   *   
   * @return an <code>Iter</code> over the elements in this array
   * @throws Exception
   */
  public abstract Iter iter() throws Exception;

  /** Returns the item at position <code>n</code> or nil if there is no such element.
   * 
   * @param n a position (0-based)
   * @return the item at position <code>n</code> or {@link Item#nil}
   * @throws Exception
   */
  public abstract Item nth(long n) throws Exception;

  /** Copies the elements of this array into <code>items</code>. The length of <code>items</code>
   * has to be identical to the length of this array as produced by {@link #count()}.
   * 
   * @param items an array
   * @throws Exception
   */
  public abstract void getTuple(Item[] items) throws Exception;

  /* @see com.ibm.jaql.json.type.JValue#copy(com.ibm.jaql.json.type.JValue) */
  @Override
  public abstract void setCopy(JValue value) throws Exception;


  // -- business methods --------------------------------------------------------------------------
  
  /** Checks whether this array contains any elements. */
  public boolean isEmpty()
  {
    return count() == 0;
  }

  /** Returns a <code>JIterator</code> over the items stored in this array.
   * @return a <code>JIterator</code> over the items stored in this array
   * @throws Exception
   */
  public final JIterator jIterator() throws Exception
  {
    return new IterJIterator(iter());
  }

  
  /* @see JValue#print(PrintStream) */
  @Override
  public void print(PrintStream out) throws Exception
  {
    iter().print(out);
  }

  /* @see com.ibm.jaql.json.type.JValue#print(java.io.PrintStream, int) */
  @Override
  public void print(PrintStream out, int indent) throws Exception
  {
    iter().print(out, indent);
  }

  /* @see JValue#toJSON() */
  @Override
  public String toJSON()
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baos);
    try
    {
      print(out);
    }
    catch (Exception e)
    {
      throw new UndeclaredThrowableException(e);
    }
    out.flush();
    return baos.toString();
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
    JArray that = (JArray) x;
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
      Item item;
      Iter iter = this.iter();
      int h = initHash();
      while ((item = iter.next()) != null)
      {
        h = hashItem(h, item);
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

  protected final static int hashItem(int h, Item item)
  {
    return (h ^ item.hashCode()) * BaseUtil.GOLDEN_RATIO_32;
  }
  
  /* @see com.ibm.jaql.json.type.JValue#longHashCode() */
  @Override
  public long longHashCode()
  {
    try
    {
      Item item;
      Iter iter = this.iter();
      long h = initLongHash();
      while ((item = iter.next()) != null)
      {
        h = longHashItem(h, item);
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

  protected final static long longHashItem(long h, Item item)
  {
    return (h ^ item.hashCode()) * BaseUtil.GOLDEN_RATIO_64;
  }
}

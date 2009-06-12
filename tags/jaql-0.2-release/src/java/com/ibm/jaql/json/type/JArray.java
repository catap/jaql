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

/**
 * 
 */
public abstract class JArray extends JValue
{
  public final static FixedJArray empty     = FixedJArray.getEmpty();
  public final static Item        emptyItem = new Item(empty);

  /**
   * @return
   */
  public boolean isEmpty()
  {
    return count() == 0;
  }

  /**
   * @return
   */
  protected final static long initHash()
  {
    return BaseUtil.GOLDEN_RATIO_64;
  }

  /**
   * @param h
   * @param item
   * @return
   */
  protected final static long hashItem(long h, Item item)
  {
    h |= item.hashCode();
    h *= BaseUtil.GOLDEN_RATIO_64;
    return h;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#equals(java.lang.Object)
   */
  public final boolean equals(Object x)
  {
    return this.compareTo(x) == 0;
  }

  /**
   * @return
   * @throws Exception
   */
  public final JIterator jiterator() throws Exception
  {
    return new IterJIterator(iter());
  }

  /**
   * Print the atom on the stream in (extended) JSON text format.
   * 
   * @param out
   */
  public void print(PrintStream out) throws Exception
  {
    iter().print(out);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#print(java.io.PrintStream, int)
   */
  public void print(PrintStream out, int indent) throws Exception
  {
    iter().print(out, indent);
  }

  /**
   * Convert the value to a string in (extended) JSON text format.
   */
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

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#compareTo(java.lang.Object)
   */
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

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#longHashCode()
   */
  @Override
  public long longHashCode()
  {
    try
    {
      Item item;
      Iter iter = this.iter();
      long h = initHash();
      while ((item = iter.next()) != null)
      {
        hashItem(h, item);
      }
      return h;
    }
    catch (Exception e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }

  /**
   * @return
   */
  public abstract long count();
  /**
   * @return
   * @throws Exception
   */
  /**
   * @return
   * @throws Exception
   */
  public abstract Iter iter() throws Exception;
  /**
   * @param n
   * @return
   * @throws Exception
   */
  public abstract Item nth(long n) throws Exception;
  /**
   * @param items
   * @throws Exception
   */
  public abstract void getTuple(Item[] items) throws Exception;
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#copy(com.ibm.jaql.json.type.JValue)
   */
  public abstract void copy(JValue value) throws Exception;
}

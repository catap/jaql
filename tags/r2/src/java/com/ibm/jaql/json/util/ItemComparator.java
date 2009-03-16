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
package com.ibm.jaql.json.util;

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.type.SpillJArray;

/**
 * 
 */
public class ItemComparator extends WritableComparator
{

  /**
   * 
   */
  public ItemComparator()
  {
    super(Item.class);
  }

  /**
   * @param walker1
   * @param walker2
   * @return
   */
  protected int compareWalkers(ItemWalker walker1, ItemWalker walker2)
  {
    int c;
    try
    {
      while (true)
      {
        int code1 = walker1.next();
        int code2 = walker2.next();

        c = walker1.type.compareTo(walker2.type);
        if (c != 0)
        {
          return c;
        }
        assert code1 == code2;

        if (code1 == ItemWalker.ATOM || code1 == ItemWalker.FIELD_NAME)
        {
          JValue v1 = walker1.getAtom();
          JValue v2 = walker2.getAtom();
          c = v1.compareTo(v2);
          if (c != 0)
          {
            return c;
          }
        }
        else if (code1 == ItemWalker.EOF)
        {
          return 0;
        }
      }
    }
    catch (IOException e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.WritableComparator#compare(byte[], int, int,
   *      byte[], int, int)
   */
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
  {
    ItemWalker walker1 = new ItemWalker();
    ItemWalker walker2 = new ItemWalker();
    walker1.reset(b1, s1, l1);
    walker2.reset(b2, s2, l2);
    return compareWalkers(walker1, walker2);
  }

  /**
   * @param in1
   * @param in2
   * @return
   */
  public int compareSpillArrays(DataInput in1, DataInput in2)
  {
    // FIXME: don't allocate each time, use a pool
    ItemWalker walker1 = new ItemWalker();
    ItemWalker walker2 = new ItemWalker();
    walker1.resetSpillArray(in1);
    walker2.resetSpillArray(in2);
    return compareWalkers(walker1, walker2);
  }

  /**
   * @param a
   * @param b
   * @return
   */
  public int compare(Item a, Item b)
  {
    try
    {
      JValue wa = a.get();
      JValue wb = b.get();
      if (wa instanceof SpillJArray && wb instanceof SpillJArray)
      {
        SpillJArray ba = (SpillJArray) wa;
        SpillJArray bb = (SpillJArray) wb;
        return compareSpillArrays(ba.getSpillFile().getInput(), bb
            .getSpillFile().getInput());
      }
      // TODO:? if( c == JMap.class )
      return a.compareTo(b);
    }
    catch (IOException ex)
    {
      throw new UndeclaredThrowableException(ex);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.WritableComparable,
   *      org.apache.hadoop.io.WritableComparable)
   */
  public int compare(WritableComparable a, WritableComparable b)
  {
    return compare((Item) a, (Item) b);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.WritableComparator#compare(java.lang.Object,
   *      java.lang.Object)
   */
  public int compare(Object a, Object b)
  {
    return compare((Item) a, (Item) b);
  }

}

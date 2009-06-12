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

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;

import org.apache.hadoop.io.WritableComparable;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JValue;

/**
 * This compares two JArrays of fixed length equal to asc.length
 */
public class AscDescItemComparator extends ItemComparator
{
  boolean[] asc;

  /**
   * asc[i] = true if ascending, false if descending
   * 
   * @param asc
   */
  public AscDescItemComparator(boolean[] asc)
  {
    this.asc = asc;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.util.ItemComparator#compare(byte[], int, int,
   *      byte[], int, int)
   */
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
  {
    int c = 0;
    int depth = 0;
    int slot = -1;

    try
    {
      ItemWalker walker1 = new ItemWalker();
      ItemWalker walker2 = new ItemWalker();
      walker1.reset(b1, s1, l1);
      walker2.reset(b2, s2, l2);
      int code1 = walker1.next();
      int code2 = walker2.next();
      if (code1 != ItemWalker.ARRAY || code2 != ItemWalker.ARRAY)
      {
        throw new RuntimeException("arrays are required for this comparator");
      }

      do
      {
        if (depth == 0)
        {
          slot++;
        }
        code1 = walker1.next();
        code2 = walker2.next();

        c = walker1.type.compareTo(walker2.type);
        if (c == 0)
        {
          assert code1 == code2;
          switch (code1)
          {
            case ItemWalker.EOF :
              assert slot == asc.length;
              assert code2 == ItemWalker.EOF;
              assert depth == -1;
              return 0;

            case ItemWalker.RECORD :
            case ItemWalker.ARRAY :
              depth++;
              break;

            case ItemWalker.END_RECORD :
            case ItemWalker.END_ARRAY :
              depth--;
              break;

            case ItemWalker.NULL :
              break;

            case ItemWalker.ATOM :
            case ItemWalker.FIELD_NAME :
              JValue v1 = walker1.getAtom();
              JValue v2 = walker2.getAtom();
              c = v1.compareTo(v2);
              break;

            default :
              assert (false);
              break;
          }
        } // if same type

      } while (c == 0);

      if (asc[slot] == false)
      {
        c = -c;
      }

      return c;
    }
    catch (IOException e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.util.ItemComparator#compare(com.ibm.jaql.json.type.Item,
   *      com.ibm.jaql.json.type.Item)
   */
  public int compare(Item a, Item b)
  {
    throw new RuntimeException("NYI");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.util.ItemComparator#compare(org.apache.hadoop.io.WritableComparable,
   *      org.apache.hadoop.io.WritableComparable)
   */
  public int compare(WritableComparable a, WritableComparable b)
  {
    throw new RuntimeException("NYI");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.util.ItemComparator#compare(java.lang.Object,
   *      java.lang.Object)
   */
  public int compare(Object a, Object b)
  {
    throw new RuntimeException("NYI");
  }
}

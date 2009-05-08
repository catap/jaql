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

import com.ibm.jaql.io.serialization.def.DefaultFullSerializer;
import com.ibm.jaql.json.type.FixedJArray;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.Item.Encoding;
import com.ibm.jaql.util.BaseUtil;

/**
 * Compares two JArrays of fixed length using a list of ascending/descending indicators
 * of the same length.
 * 
 * This class is not threadsafe. It can be used only with Hadoop version 0.18.0 and above because
 * earlier versions shared comparators between threads.
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

  /** Ascending/descending indicator-aware item comparison. This methods works like 
   * {@link ItemComparator#compare(Item, Item)} but enforces that its inputs corresponds
   * to arrays of length <code>asc.length</code> and uses 
   * {@link #compareArraysAscDesc(DataInput, int, DataInput, int)} to perform the array comparison.
   */
  protected int compareItemsAscDesc(DataInput input1, DataInput input2) throws IOException {
    // TODO: this code breaks with other serializers or when serialization is changed
    assert serializer instanceof DefaultFullSerializer;
    
    // read and compare encodings / types
    int code1 = BaseUtil.readVUInt(input1);
    int code2 = BaseUtil.readVUInt(input2);
    assert code1>0 && code2>0;
    Item.Encoding encoding1 = Item.Encoding.valueOf(code1);
    Item.Encoding encoding2 = Item.Encoding.valueOf(code2);

    // check that we have arrays and read their length
    long l1 = 0;
    if (encoding1.equals(Encoding.ARRAY_FIXED)) {
      l1 = BaseUtil.readVUInt(input1);
    } else if (encoding1.equals(Encoding.ARRAY_SPILLING)) {
      l1 = BaseUtil.readVULong(input1);
    } else {
      throw new RuntimeException("Input types must be arrays");
    }
    
    long l2 = 0;
    if (encoding2.equals(Encoding.ARRAY_FIXED)) {
      l2 = BaseUtil.readVUInt(input2);
    } else if (encoding2.equals(Encoding.ARRAY_SPILLING)) {
      l2 = BaseUtil.readVULong(input2);
    } else {
      throw new RuntimeException("Input types must be arrays");
    }
    
    // check length
    if (l1 != asc.length || l2 != asc.length) {
      throw new RuntimeException("Arrays must have same lengths as asc/desc indicators");
    }
    
    // compare values
    int cmp = compareArraysAscDesc(input1, input2);

    return cmp;
  }

  /** Ascending/descending indicator-aware array comparison. Assumes that its inputs point
   * to the first items of two arrays of length <code>asc.length</code> */
  protected int compareArraysAscDesc(DataInput input1, DataInput input2) 
  throws IOException {
    for (int i=0; i<asc.length; i++) {
      int cmp = serializer.compare(input1, input2); // normal comparison
      if (cmp != 0) {
        return asc[i] ? cmp : -cmp;
      }
    }
    return 0;
  }
  
  /* @see com.ibm.jaql.json.util.ItemComparator#compare(byte[], int, int, byte[], int, int) */
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
  {
    try
    {
      input1.reset(b1, s1, l1);
      input2.reset(b2, s2, l2);
      return compareItemsAscDesc(input1, input2);
    } catch (IOException e)
    {
      throw new RuntimeException(e);
    }
  }

  /* @see com.ibm.jaql.json.util.ItemComparator#compare(com.ibm.jaql.json.type.Item,
   *                                                    com.ibm.jaql.json.type.Item)  */
  public int compare(Item a, Item b)
  {
    // TODO: should this work with any JArray?
    // TODO: should this eliminate Item and use FixedJArray directly?
    // TODO: can this assume non-null?
    FixedJArray x = (FixedJArray)a.get();
    FixedJArray y = (FixedJArray)b.get();
    if( x == null )
    {
      if( y == null )
      {
        return 0;
      }
      return -1;
    }
    else if( y == null )
    {
      return 1;
    }
    if( x.size() != asc.length ||
        y.size() != asc.length )
    {
      throw new RuntimeException("Arrays must have same lengths as asc/desc indicators");
    }
    for(int i = 0 ; i < asc.length ; i++)
    {
      int c = x.get(i).compareTo(y.get(i));
      if( c != 0 )
      {
        return asc[i] ? c : -c;
      }
    }
    return 0;
  }

}

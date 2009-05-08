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

import org.apache.hadoop.io.DataInputBuffer;

import com.ibm.jaql.io.serialization.FullSerializer;
import com.ibm.jaql.io.serialization.def.DefaultFullSerializer;
import com.ibm.jaql.io.serialization.def.JStringSerializer;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.Item.Encoding;
import com.ibm.jaql.lang.core.JComparator;

/** Hadoop-compatible comparator for two JSON values. The comparator makes use of 
 * the total order of all types (to determine order of elements of different types). 
 * 
 * This class is not threadsafe. It can be used only with Hadoop version 0.18.0 and above because
 * earlier versions shared comparators between threads.
 */
public class ItemComparator implements JComparator
{
  // type of serialization; currently hardcoded
  protected FullSerializer serializer = FullSerializer.getDefault();
  
  // cache variables for hashing  
  protected DataInputBuffer buffer = new DataInputBuffer();
  protected Item key1 = new Item();
  
  // cache variables for comparing
  protected DataInputBuffer input1 = new DataInputBuffer();
  protected DataInputBuffer input2 = new DataInputBuffer();
  protected JStringSerializer jstringSerializer 
    = (JStringSerializer)DefaultFullSerializer.getDefaultInstance().getSerializer(Encoding.STRING);

  // -- comparison -------------------------------------------------------------------------------

  /* @see org.apache.hadoop.io.WritableComparator#compare(byte[], int, int, byte[], int, int) */
  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
  {
    try
    {
      input1.reset(b1, s1, l1);
      input2.reset(b2, s2, l2);
      return serializer.compare(input1, input2);
    } catch (IOException e)
    {
      throw new RuntimeException(e);
    }
  }

  /* @see org.apache.hadoop.io.WritableComparator#compare(Object, Object) */
  @Override
  public int compare(Item a, Item b)
  {
    return a.compareTo(b);
  }

  
  // -- hashing (currently unused) ---------------------------------------------------------------

  @Override
  public long longHash(byte[] bytes, int offset, int length) throws IOException
  {
    buffer.reset(bytes, offset, length);
    key1.readFields(buffer);
    longHash(key1);
    return 0;
  }

  @Override
  public long longHash(Item key)
  {
    return key1.longHashCode();
  }

}

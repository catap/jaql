/*
 * Copyright (C) IBM Corp. 2009.
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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.ibm.jaql.json.type.Item;

/**
 * This class only exists because WritableComparator does not pass its type parameter
 * to RawComparator properly.  Therefore, ItemComparator cannot extend WritableComparator
 * unless it also leaves RawComparator type parameter unspecified.
 * 
 * @author kbeyer
 *
 */
public class ItemWritableComparator extends WritableComparator
{
  final ItemComparator cmp = new ItemComparator();
  
  public ItemWritableComparator()
  {
    super(Item.class);
  }

  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
  {
    return cmp.compare(b1, s1, l1, b2, s2, l2);
  }

  @Override
  public int compare(WritableComparable a, WritableComparable b)
  {
    return cmp.compare((Item)a, (Item)b);
  }
  
  @Override
  public int compare(Object a, Object b)
  {
    return cmp.compare((Item)a, (Item)b);
  }

  @Override
  public Class<Item> getKeyClass()
  {
    return Item.class;
  }

  @Override
  public Item newKey()
  {
    return new Item();
  }
}

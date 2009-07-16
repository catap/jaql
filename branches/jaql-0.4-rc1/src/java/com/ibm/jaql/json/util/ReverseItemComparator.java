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

import com.ibm.jaql.json.type.Item;

/**
 * 
 */
public class ReverseItemComparator extends ItemComparator
{
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.util.ItemComparator#compare(byte[], int, int,
   *      byte[], int, int)
   */
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
  {
    return -super.compare(b1, s1, l1, b2, s2, l2);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.util.ItemComparator#compare(com.ibm.jaql.json.type.Item,
   *      com.ibm.jaql.json.type.Item)
   */
  public int compare(Item a, Item b)
  {
    return -super.compare(a, b);
  }

}

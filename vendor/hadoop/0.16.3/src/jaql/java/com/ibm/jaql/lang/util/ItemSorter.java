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
package com.ibm.jaql.lang.util;

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.PublicMergeSorter;
import org.apache.hadoop.mapred.Reporter;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.lang.core.JComparator;

/**
 * 
 */
public class ItemSorter
{
  DataOutputBuffer       keyValBuffer = new DataOutputBuffer();
  PublicMergeSorter      sorter       = new PublicMergeSorter();
  RawKeyValueIterator    iter;
  DataInputBuffer        valIn        = new DataInputBuffer();
  DataOutputBuffer       valOut       = new DataOutputBuffer();
  Item                   value        = new Item();

  private static JobConf conf         = new JobConf();
  static
  {
    conf.setMapOutputKeyClass(Item.class);
  }

  /**
   * @param comparator
   */
  public ItemSorter(JComparator comparator)
  {
    // JobConf conf = new JobConf();
    // conf.setMapOutputKeyClass(Item.class);
    sorter.configure(conf);
    sorter.setInputBuffer(keyValBuffer);
    sorter.setProgressable(Reporter.NULL);
    if (comparator != null)
    {
      sorter.setComparator(comparator);
    }
  }

  /**
   * 
   */
  public ItemSorter()
  {
    this(null);
  }

  /**
   * @param key
   * @param value
   * @throws IOException
   */
  public void add(Item key, Item value) throws IOException
  {
    int keyOffset = keyValBuffer.getLength();
    key.write(keyValBuffer);
    int keyLength = keyValBuffer.getLength() - keyOffset;
    value.write(keyValBuffer);
    int valLength = keyValBuffer.getLength() - (keyOffset + keyLength);
    sorter.addKeyValue(keyOffset, keyLength, valLength);
    // TODO: spill to disk
  }

  /**
   * 
   */
  public void sort()
  {
    iter = sorter.sort(); // warning: sort() returns null if no records to sort.
  }

  /**
   * @return
   * @throws IOException
   */
  public Item nextValue() throws IOException
  {
    if (iter == null || !iter.next())
    {
      return null;
    }

    valOut.reset();
    iter.getValue().writeUncompressedBytes(valOut);
    valIn.reset(valOut.getData(), valOut.getLength());
    value.readFields(valIn);

    return value;
  }
}

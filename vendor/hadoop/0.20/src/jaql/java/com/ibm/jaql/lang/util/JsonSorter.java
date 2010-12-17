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

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.OutputBuffer;
import org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.PublicMergeSorter;
import org.apache.hadoop.mapred.Reporter;

import com.ibm.jaql.io.hadoop.HadoopSerializationDefault;
import com.ibm.jaql.io.hadoop.JsonHolderDefault;
import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.DefaultJsonComparator;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.JsonComparator;

/**
 * 
 */
public class JsonSorter
{
  OutputBuffer  keyValBuffer = new OutputBuffer();

  DataOutputStream keyValStream = new DataOutputStream(keyValBuffer);

  // DataOutputBuffer       keyValBuffer = new DataOutputBuffer();

  PublicMergeSorter          sorter       = new PublicMergeSorter();

  RawKeyValueIterator    iter;

  DataInputBuffer        valIn        = new DataInputBuffer();

  DataOutputBuffer       valOut       = new DataOutputBuffer();

  private JobConf conf = new JobConf();

  BinaryFullSerializer serializer = BinaryFullSerializer.getDefault();

  /**
   * @param comparator
   */
  public JsonSorter(JsonComparator comparator)
  {
    conf.setMapOutputKeyClass(JsonHolderDefault.class);
    HadoopSerializationDefault.register(conf);
    if (comparator != null)
    {
      conf.setOutputKeyComparatorClass(comparator.getClass());      
    }
    else 
    {
      conf.setOutputKeyComparatorClass(DefaultJsonComparator.class);
    }
//    sorter.configure(conf); // done below using setComparator    
    sorter.setInputBuffer(keyValBuffer);
    sorter.setProgressable(Reporter.NULL);
    if (comparator != null)
    {
      sorter.setComparator(comparator);
    }
    else
    {
      sorter.setComparator(new DefaultJsonComparator());
    }
  }

  /**
   * 
   */
  public JsonSorter()
  {
    this(null);
  }

  /**
   * @param key
   * @param value
   * @throws IOException
   */
  public void add(JsonValue key, JsonValue value) throws IOException
  {
    int keyOffset = keyValBuffer.getLength();
    serializer.write(keyValStream, key);
    int keyLength = keyValBuffer.getLength() - keyOffset;
    serializer.write(keyValStream, value);
    int valLength = keyValBuffer.getLength() - (keyOffset + keyLength);
    sorter.addKeyValue(keyOffset, keyLength, valLength);
    
//    int keyOffset = keyValBuffer.getLength();
//    serializer.write(keyValBuffer, key);
//    int keyLength = keyValBuffer.getLength() - keyOffset;
//    serializer.write(keyValBuffer, value);
//    int valLength = keyValBuffer.getLength() - (keyOffset + keyLength);
//    obuf.write(keyValBuffer.getData(), keyOffset, keyLength + valLength);
//    sorter.addKeyValue(keyOffset, keyLength, valLength);
//    // keyValBuffer.reset();
//    // FIXME: we are double buffering here. the appropriate manner is to use
//    // Item's serializer.
//    // TODO: spill to disk
  }

  /**
   * 
   */
  public void sort()
  {
    iter = sorter.sort(); // warning: sort() returns null if no records to sort.
  }

  public JsonIterator iter() {
    if (iter == null) {
      // If no records were added, return []
      return JsonIterator.EMPTY;
    }
    return new JsonIterator() {

      @Override
      protected boolean moveNextRaw() throws Exception
      {
        if (!iter.next()) 
        {
          return false;
        }
        
        valOut.reset();
        iter.getValue().writeUncompressedBytes(valOut);
        valIn.reset(valOut.getData(), valOut.getLength());
        currentValue = serializer.read(valIn, currentValue);
        return true;
      }      
    };
  }  
}

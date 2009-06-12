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
package com.ibm.jaql.lang.expr.hadoop;

import org.apache.hadoop.mapred.RecordReader;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.util.Iter;


public class RecordReaderValueIter extends Iter
{
  protected RecordReader<Item,Item> reader;
  protected Item key;
  protected Item value;
  
  public RecordReaderValueIter(RecordReader<Item,Item> reader)
  {
    this.reader = reader;
    this.key = reader.createKey();
    this.value = reader.createValue();
  }

  @Override
  public Item next() throws Exception
  {
    if( reader.next(key, value) )
    {
      return value;
    }
    return null;
  }

}

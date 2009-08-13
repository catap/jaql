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

import com.ibm.jaql.io.hadoop.JsonHolder;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.util.JsonIterator;


public class RecordReaderKeyValueIter extends JsonIterator
{
  public static final JsonString KEY   = new JsonString("key");
  public static final JsonString VALUE = new JsonString("value");
  
  protected RecordReader<JsonHolder, JsonHolder> reader;
  protected JsonHolder key;
  protected JsonHolder value;
  protected BufferedJsonRecord pair;
  
  public RecordReaderKeyValueIter(RecordReader<JsonHolder, JsonHolder> reader)
  {
    this.reader = reader;
    this.key = reader.createKey();
    this.value = reader.createValue();
    if( key == null )
    {
      key = new JsonHolder();
    }
    if( value == null )
    {
      value = new JsonHolder();
    }
    pair = new BufferedJsonRecord(2);
  }

  @Override
  public boolean moveNext() throws Exception
  {
    if( reader.next(key, value) )
    {
      pair.set(KEY,   key.value);
      pair.set(VALUE, value.value);
      currentValue = pair;
      return true;
    }
    return false;
  }

}

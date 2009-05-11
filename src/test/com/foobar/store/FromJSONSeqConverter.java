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
package com.foobar.store;

import java.io.ByteArrayInputStream;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.ibm.jaql.io.converter.ToItem;
import com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem;
import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.type.Item;

/**
 * 
 */
public class FromJSONSeqConverter extends HadoopRecordToItem
{

  /**
   * @param w
   * @param i
   */
  private void convertWritableToItem(Writable w, Item i)
  {
    if (w == null || i == null) return;
    Text t = null;
    if (w instanceof Text)
    {
      t = (Text) w;
    }
    else
    {
      t = new Text(w.toString());
    }

    ByteArrayInputStream input = new ByteArrayInputStream(t.getBytes());
    JsonParser parser = new JsonParser(input);

    try
    {
      Item data = parser.JsonVal();
      i.set(data.get());
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem#createKeyConverter()
   */
  @Override
  protected ToItem<WritableComparable> createKeyConverter()
  {
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem#createValConverter()
   */
  @Override
  protected ToItem<Writable> createValConverter()
  {
    return new ToItem<Writable>()
    {

      public void convert(Writable src, Item tgt)
      {
        convertWritableToItem(src, tgt);
      }

      public Item createTarget()
      {
        return new Item();
      }
    };
  }

}

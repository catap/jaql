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
package com.ibm.jaql.io.hadoop.converter;

import java.io.StringReader;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

import com.ibm.jaql.io.converter.ToItem;
import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.parser.ParseException;
import com.ibm.jaql.json.type.Item;

/**
 * 
 */
public class FromJSONTxtConverter extends HadoopRecordToItem<WritableComparable, Text>
{

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
  protected ToItem<Text> createValConverter()
  {
    return new ToItem<Text>() {
      JsonParser parser = new JsonParser();
      
      public void convert(Text src, Item tgt)
      {
        if (src == null || tgt == null) return;
        
        try
        {
          parser.ReInit(new StringReader(src.toString()));
          Item data = parser.JsonVal();
          tgt.set(data.get());
        }
        catch (ParseException pe)
        {
          throw new RuntimeException(pe);
        }
      }

      public Item createTarget()
      {
        return new Item();
      }

    };
  }
}

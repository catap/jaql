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
package com.acme.extensions.data;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem;
import com.ibm.jaql.io.hadoop.converter.WritableComparableToItem;
import com.ibm.jaql.io.hadoop.converter.WritableToItem;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JString;

/**
 * Assumes that the "value" of a [key, value] pair is of type
 * o.a.h.io.Text. For each such value, this converter returns a JString.
 */
public class FromLineConverter extends HadoopRecordToItem {
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem#createKeyConverter()
   */
  @Override
  protected WritableComparableToItem createKeyConverter()
  {
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem#createValConverter()
   */
  @Override
  protected WritableToItem createValConverter()
  {
    return new WritableToItem() {
      public void convert(Writable src, Item tgt)
      {
        if (src == null || tgt == null) return;
        Text t = null;
        if (src instanceof Text)
        {
          t = (Text) src;
        }
        else
        {
          throw new RuntimeException("tried to convert from: " + src);
        }
        ((JString)tgt.getNonNull()).set(t.getBytes());
      }
      
      public Item createTarget()
      {
        return new Item(new JString());
      }

    };
  }

  @Override
  public Item createTarget()
  {
    return new Item(new JString());
  }
  
  
}
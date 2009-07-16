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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.ibm.jaql.io.converter.ToItem;
import com.ibm.jaql.io.hadoop.converter.ItemToHadoopRecord;
import com.ibm.jaql.json.type.Item;

/**
 * 
 */
public class ToJSONSeqConverter extends ItemToHadoopRecord
{
  /**
   * @param i
   * @param t
   */
  private void convertItemToText(Item i, Text t)
  {
    if (i == null || t == null) return;

    try
    {
      String s = convertItemToString(i);
      t.set(s.getBytes());
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }

  /**
   * @param i
   * @return
   * @throws Exception
   */
  protected String convertItemToString(Item i) throws Exception
  {
    return i.toJSON();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.converter.ItemToHadoopRecord#createKeyConverter()
   */
  @Override
  protected ToItem<WritableComparable> createKeyConverter()
  {
    return null;//new FromItemComp();
  }
  
  /* (non-Javadoc)
   * @see com.ibm.jaql.io.hadoop.converter.ItemToHadoopRecord#createKeyTarget()
   */
  @Override
  public WritableComparable createKeyTarget() 
  {
    return new Text();
  };

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.converter.ItemToHadoopRecord#createValConverter()
   */
  @Override
  protected ToItem<Writable> createValConverter()
  {
    return new ToItem<Writable>()
    {

      /*
       * (non-Javadoc)
       * 
       * @see com.ibm.jaql.io.converter.ToItem#convert(com.ibm.jaql.json.type.Item,
       *      java.lang.Object)
       */
      public void convert(Item src, Writable tgt)
      {
        convertItemToText(src, (Text) tgt);
      }

      /*
       * (non-Javadoc)
       * 
       * @see com.ibm.jaql.io.converter.ToItem#createTarget()
       */
      public Writable createTarget()
      {
        return new Text();
      }
    };
  }
}

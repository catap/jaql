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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

import com.ibm.jaql.io.converter.StreamToItem;
import com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem;
import com.ibm.jaql.io.hadoop.converter.WritableComparableToItem;
import com.ibm.jaql.io.hadoop.converter.WritableToItem;
import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.type.Item;

/**
 * 
 */
public class FromJSONTxtConverter extends HadoopRecordToItem
    implements
      StreamToItem
{

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

        ByteArrayInputStream input = new ByteArrayInputStream(t.getBytes());
        setInputStream(input);
        try
        {
          read(tgt);
          close();
        }
        catch (IOException ioe)
        {
          throw new RuntimeException(ioe);
        }
      }

      public Item createTarget()
      {
        return new Item();
      }

    };
  }

  private InputStream input;

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.converter.StreamToItem#setInputStream(java.io.InputStream)
   */
  public void setInputStream(InputStream in)
  {
    this.input = in;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.converter.StreamToItem#read(com.ibm.jaql.json.type.Item)
   */
  public boolean read(Item v) throws IOException
  {
    JsonParser parser = new JsonParser(input);
    try
    {

      Item data = parser.JsonVal();
      v.set(data.get());
      return true;
    }
    catch (Exception eof)
    {
      return false;
    }
  }

  /**
   * @throws IOException
   */
  public void close() throws IOException
  {
    input.close();
  }
}

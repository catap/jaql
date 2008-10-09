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
package com.ibm.jaql.io.stream.converter;

import java.io.IOException;
import java.io.InputStream;

import com.ibm.jaql.io.converter.StreamToItem;
import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.parser.ParseException;
import com.ibm.jaql.json.type.Item;

/**
 * 
 */
public class JSONTextInputStream implements StreamToItem
{

  private JsonParser parser;

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.converter.StreamToItem#createTarget()
   */
  public Item createTarget()
  {
    return new Item();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.converter.StreamToItem#read(com.ibm.jaql.json.type.Item)
   */
  public boolean read(Item v) throws IOException
  {
    try
    {
      Item i = parser.JsonVal();
      v.set(i.get());
      return true;
    }
    catch (ParseException ex)
    {
      return false;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.converter.StreamToItem#setInputStream(java.io.InputStream)
   */
  public void setInputStream(InputStream in)
  {
    parser = new JsonParser(in);
  }

}

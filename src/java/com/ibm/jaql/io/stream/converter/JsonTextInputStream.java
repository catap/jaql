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

import com.ibm.jaql.io.converter.StreamToJson;
import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.parser.ParseException;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonValue;

/** Parses a JSON file and returns its representation as {@link Item}s. 
 * 
 */
public class JsonTextInputStream implements StreamToJson<JsonValue>
{
  private boolean    arrAcc = true;
  private boolean    firstPass = true;
  private JsonParser parser;

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.converter.StreamToItem#setInputStream(java.io.InputStream)
   */
  public void setInput(InputStream in)
  {
    parser = new JsonParser(in);
  }
  
  /* (non-Javadoc)
   * @see com.ibm.jaql.io.converter.StreamToItem#setArrayAccessor(boolean)
   */
  public void setArrayAccessor(boolean a) 
  {
    arrAcc = a;
  }
  
  /* (non-Javadoc)
   * @see com.ibm.jaql.io.converter.StreamToItem#isArrayAccessor()
   */
  public boolean isArrayAccessor()
  {
    return arrAcc;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.converter.StreamToItem#read(com.ibm.jaql.json.type.Item)
   */
  public JsonValue read(JsonValue v) throws IOException
  {
    try
    {
      JsonValue i = null;
      if(arrAcc) {
        if(firstPass) {
          i = parser.ArrayFirst();
          firstPass = false;
        } else {
          i = parser.ArrayNext();
        }
      } else {
        i = parser.JsonVal();
      }
      if(i == JsonParser.NIL) 
        return null;
      return i;
    }
    catch (ParseException ex)
    {
      return null;
    }
  }
  
  public Schema getSchema()
  {
    return SchemaFactory.anyOrNullSchema();
  }
}

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.ibm.jaql.io.converter.StreamToJson;
import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.parser.ParseException;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonBinary;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;

/** Parses a JSON file and returns its representation as {@link Item}s. 
 * 
 */
public class LineTextInputStream implements StreamToJson<JsonValue>
{
  private boolean    arrAcc = true;
  private InputStream isReader = null;
  private BufferedReader r = null;

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.converter.StreamToItem#setInputStream(java.io.InputStream)
   */
  public void setInput(InputStream in)
  {
	  isReader = in;
	  r = new BufferedReader(new InputStreamReader(isReader));
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
	  	String tmp = null;
	    if((tmp = r.readLine()) != null)  
	    	return new JsonString(tmp);	    
	  	return null;
  }
  
  public Schema getSchema()
  {
    return SchemaFactory.anySchema();
  }
}

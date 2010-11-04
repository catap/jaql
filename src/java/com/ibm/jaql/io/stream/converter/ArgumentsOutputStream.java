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
import java.io.OutputStream;

import com.ibm.jaql.io.converter.JsonToStream;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.FastPrintStream;

/** Writes serialized {@link JsonValue}s to a binary output stream.
 * 
 */
public class ArgumentsOutputStream implements JsonToStream<JsonValue>
{
  protected FastPrintStream output;
  private boolean          arrAcc = true;
  //private com.ibm.jaql.io.serialization.text.TextFullSerializer serializer = com.ibm.jaql.io.serialization.text.TextFullSerializer.getDefault();
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.converter.ItemToStream#setOutputStream(java.io.OutputStream)
   */
  public void setOutputStream(OutputStream out)
  {
    output = new FastPrintStream(out);
  }
  
  /* (non-Javadoc)
   * @see com.ibm.jaql.io.converter.ItemToStream#setArrayAccessor(boolean)
   */
  public void setArrayAccessor(boolean a) {
    arrAcc = a;
  }
  
  /* (non-Javadoc)
   * @see com.ibm.jaql.io.converter.ItemToStream#isArrayAccessor()
   */
  public boolean isArrayAccessor() {
    return arrAcc;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.converter.ItemToStream#write(com.ibm.jaql.json.type.Item)
   */
  public void write(JsonValue i) throws IOException
  {	
	  output.print(processArguments(i));
	  output.close();
  }
  
  protected StringBuffer processArguments(JsonValue value) throws IOException
  {
    StringBuffer sb = new StringBuffer();
    if(value instanceof JsonArray)
    {	    	    
      for(JsonValue m :(JsonArray)value)
      {	    		
        sb.append(m.toString());
        sb.append(" ");
      }
      if(sb.length() > 0)
        sb = sb.deleteCharAt(sb.length()-1);
    }
    else
    {
      sb.append(value.toString());
    }
    return sb;
  }
  
  @Override
  public void flush() throws IOException
  {
    if (output != null)
    {
      output.flush();
    }
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.converter.ItemToStream#close()
   */
  public void close() throws IOException
  {
    if (output != null)
    {
      output.flush();
      output.close();
    }
  }
  
  @Override
  public void init(JsonValue options) throws Exception {}
}

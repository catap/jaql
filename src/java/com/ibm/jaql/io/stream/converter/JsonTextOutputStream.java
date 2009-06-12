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
import java.io.PrintStream;

import com.ibm.jaql.io.converter.JsonToStream;
import com.ibm.jaql.json.type.JsonValue;

/** Writes serialized {@link Item}s to a binary output stream.
 * 
 */
public class JsonTextOutputStream implements JsonToStream<JsonValue>
{
  private PrintStream output;
  private boolean     arrAcc = true;
  private boolean     seenFirst = false;
  private String      ARR_OPEN  = "[";
  private String      ARR_CLOSE = "]";
  private String      ARR_SEP   = ",";
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.converter.ItemToStream#setOutputStream(java.io.OutputStream)
   */
  public void setOutputStream(OutputStream out)
  {
    output = new PrintStream(out);
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
    try {
      if(seenFirst && !arrAcc)
        throw new RuntimeException("Expected only one value when not in array mode");
      if(!seenFirst && arrAcc) {
        output.print(ARR_OPEN);
      }
      if(seenFirst)
        output.print(ARR_SEP);
      else
        seenFirst = true;
      JsonValue.print(output, i);
    } catch(Exception e) {
      throw new RuntimeException(e);
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
      if(seenFirst && arrAcc)
        output.print(ARR_CLOSE);
      output.flush();
      output.close();
    }
  }
}

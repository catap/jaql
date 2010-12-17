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
package com.ibm.jaql.io.serialization.text;

import java.io.IOException;
import java.io.InputStream;

import com.ibm.jaql.io.serialization.FullSerializer;
import com.ibm.jaql.io.serialization.text.def.DefaultTextFullSerializer;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.FastPrinter;

/** Full serializer for character data.
 * 
 * @param <T> type of value to work on
 */
public abstract class TextFullSerializer extends FullSerializer<InputStream, FastPrinter>
{
  
  @Override
  public void write(FastPrinter out, JsonValue value) throws IOException
  {
    write(out, value, 0);
  }
  
  public abstract void write(FastPrinter out, JsonValue value, int indent) throws IOException;
  
  // -- default serializer  ----------------------------------------------------------------------
  
  private static TextFullSerializer DEFAULT_SERIALIZER = DefaultTextFullSerializer.getInstance();
  private static TextFullSerializer DEFAULT_ORIGINEMITTING_SERIALIZER = 
	  DefaultTextFullSerializer.getEmittingInstance();
  
  public static TextFullSerializer getDefault()
  {
    return DEFAULT_SERIALIZER;
  }
  
  public static TextFullSerializer getDefault(boolean emitOrigin)
  {
	if(emitOrigin)
      return DEFAULT_ORIGINEMITTING_SERIALIZER;
	else
	  return DEFAULT_SERIALIZER;	
  }
}

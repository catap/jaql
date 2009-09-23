/*
 * Copyright (C) IBM Corp. 2009.
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
package com.ibm.jaql.io.hadoop;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.UndeclaredThrowableException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import com.ibm.jaql.io.converter.AbstractFromDelConverter;
import com.ibm.jaql.io.hadoop.converter.KeyValueExport;
import com.ibm.jaql.io.serialization.text.basic.BasicTextFullSerializer;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.RandomAccessBuffer;

// TODO: Does this really need to be in the vendor directory?

/** Converts JSON array or record to a delimited text value. */
public final class ToDelConverter implements KeyValueExport<NullWritable, Text>
{
  //   public static final JsonString DELIMITER_NAME = new JsonString("delimiter");

  protected JsonString[] fields;
  protected String delimiter;
  protected final BasicTextFullSerializer serializer = new BasicTextFullSerializer();
  protected final RandomAccessBuffer buffer = new RandomAccessBuffer();
  protected final PrintStream output = new PrintStream(buffer);
  
  @Override
  public void init(JsonRecord options) 
  {
    JsonString js = (JsonString)options.get(AbstractFromDelConverter.DELIMITER_NAME);
    delimiter = (js == null) ? "," : js.toString();
    
    JsonArray fieldsArr = (JsonArray)options.get(AbstractFromDelConverter.FIELDS_NAME);
    if( fieldsArr == null )
    {
      fields = null;
    }
    else
    {
      try
      {
        int n = (int)fieldsArr.count();
        fields = new JsonString[n];
        for( int i = 0 ; i < n ; i++ )
        {
          fields[i] = (JsonString)fieldsArr.get(i);
        }
      }
      catch (Exception e)
      {
        throw new UndeclaredThrowableException(e);
      }
    }
  }

  @Override
  public NullWritable createKeyTarget()
  {
    return null;
  }

  @Override
  public Text createValueTarget()
  {
    return new Text();
  }

  /** Converts the given line into a JSON value. 
   * @throws IOException */
  @Override
  public void convert(JsonValue src, NullWritable key, Text text)
  {
    try
    {
      buffer.reset();
      String sep = "";
      if( src instanceof JsonRecord )
      {
        JsonRecord rec = (JsonRecord)src;
        for( JsonString n: fields )   // fields are required for records to define order
        {
          output.print(sep);
          JsonValue value = rec.get(n);
          serializer.write(output, value);
          sep = delimiter;
        }
      }
      else if( src instanceof JsonArray )
      {
        JsonArray arr = (JsonArray)src;
        for( JsonValue value: arr )
        {
          output.print(sep);
          serializer.write(output, value);
          sep = delimiter;
        }
      }
      else
      {
        throw new ClassCastException("Type cannot be placed in delimited file.  Array or record expected: "+src);
      }
      output.flush();
      text.set(buffer.getBuffer(), 0, buffer.size());
    }
    catch( Exception e )
    {
      throw new UndeclaredThrowableException(e);
    }
  }

}
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
package com.ibm.jaql.io.stream.converter;

import java.io.IOException;
import java.io.OutputStream;

import javax.xml.stream.XMLStreamException;

import com.ibm.jaql.io.converter.JsonToStream;
import com.ibm.jaql.io.xml.JsonToXml;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonString;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.FastPrintStream;
import com.ibm.jaql.util.FastPrinter;

/**
 * A converter to convert a JSON value to a XML file.
 */
public class XmlTextOutputStream implements JsonToStream<JsonValue> // extends AbstractJsonTextOutputStream
{
  protected JsonToXml converter = new JsonToXml();
  protected FastPrinter writer;
  protected long line = 0;
  protected boolean isArray = true;
  protected MutableJsonString result = new MutableJsonString();

  
  public XmlTextOutputStream() throws Exception
  {
    converter = new JsonToXml();
  }
  
  @Override
  public void init(JsonValue options) throws Exception
  {
  }
  
  @Override
  public boolean isArrayAccessor()
  {
    return isArray;
  }

  @Override
  public void setArrayAccessor(boolean isArray)
  {
    this.isArray = isArray;
  }

  @Override
  public void setOutputStream(OutputStream out)
  {
    try
    {
      writer = new FastPrintStream(out);
      converter.setWriter(writer);
      line = 0;
    }
    catch (Exception e)
    {
      throw JaqlUtil.rethrow(e);
    }
  }
  
  
  @Override
  public void write(JsonValue value) throws IOException
  {
    try 
    {
      if( line == 0 )
      {
        converter.startDocument();
        if( isArrayAccessor() )
        {
          converter.newline();
          converter.startArrayElement();
        }
      }
      else if( ! isArrayAccessor() )
      {
        throw new IOException("Expected only one value when not in array mode");
      }
      line++;
      converter.newline();
      converter.toXml(value);
    }
    catch (IOException e) 
    {
      throw e;
    }
    catch (Exception e) 
    {
      throw new IOException(e);
    }
  }
  
  @Override
  public void flush() throws IOException
  {
    try
    {
      converter.flush();
    }
    catch (XMLStreamException e)
    {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException
  {
    try
    {
      if( isArrayAccessor() )
      {
        converter.newline();
        converter.endArrayElement();
      }
      converter.newline();
      converter.endDocument();
      converter.close();
      writer.close();
    }
    catch (XMLStreamException e)
    {
      throw new IOException(e);
    }
  }
}

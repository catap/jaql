/*
 * Copyright (C) IBM Corp. 2010.
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
package com.ibm.jaql.io.xml;

import java.io.StringWriter;
import java.io.Writer;
import java.util.Map.Entry;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonAtom;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;

public class JsonToXml
{
  protected XMLOutputFactory xfactory;
  protected XMLStreamWriter xwriter;
  

  public JsonToXml() throws Exception
  {
    xfactory = XMLOutputFactory.newInstance();
  }
  
  public void setWriter(Writer writer) throws XMLStreamException
  {
    xwriter = xfactory.createXMLStreamWriter(writer);
  }

  public void startDocument() throws XMLStreamException
  {
    xwriter.writeStartDocument();
  }

  public void startArrayElement() throws XMLStreamException
  {
    xwriter.writeStartElement( JsonType.ARRAY.toString() );
  }

  public void endArrayElement() throws XMLStreamException
  {
    xwriter.writeEndElement();
  }

  public void endDocument() throws XMLStreamException
  {
    xwriter.writeEndDocument();
  }
  
  public void close() throws XMLStreamException
  {
    xwriter.close();
    xwriter = null;
  }

  /**
   * Write the {@code value} in XML format to the given {@code writer}.
   */
  public void toXml(JsonValue value) throws Exception
  {
    toXmlAux( null, value );
  }
  
  /**
   * Write the {@code iter} in XML format to the given {@code writer}.
   */
  public void toXml(JsonIterator iter) throws Exception
  {
    startArrayElement();
    //xwriter.writeStartElement( JsonType.ARRAY.toString() );
    for( JsonValue elem: iter )
    {
      toXmlAux( null, elem );
    }
    endArrayElement();
  }
  
  /**
   * Start an element.  The name is tag if non-null, otherwise
   * it is "value", "array", or "record".  Unless the value is
   * obviously an array or record, then the start tag also
   * includes a type="type" attribute.
   */
  protected void startElement(String tag, JsonValue value) throws Exception
  {
    if( tag == null )
    {
      if( value instanceof JsonArray || value instanceof JsonRecord )
      {
        xwriter.writeStartElement( value.getType().toString() );
        return;
      }
      tag = "value";
    }
    JsonType type = value == null ? JsonType.NULL : value.getType();
    xwriter.writeStartElement( tag );
    xwriter.writeAttribute( "type", type.toString() );
  }
  
  
  /**
   * Recursively write value to our current xml writer.
   */
  protected void toXmlAux(String tag, JsonValue value) throws Exception
  {
    startElement(tag, value);
    if( value instanceof JsonAtom )
    {
      xwriter.writeCharacters( value.toString() );
    }
    else if( value == null )
    {
      // empty element
    }
    else if( value instanceof JsonRecord )
    {
      for( Entry<JsonString, JsonValue> field: (JsonRecord)value )
      {
        toXmlAux( field.getKey().toString(), field.getValue() );
      }
    }
    else if( value instanceof JsonArray )
    {
      for( JsonValue elem: (JsonArray)value )
      {
        toXmlAux( null, elem );
      }
    }
    else
    {
      throw new RuntimeException("unknown value type: "+value);
    }
    xwriter.writeEndElement();
  }

  public void newline() throws XMLStreamException
  {
    xwriter.writeCharacters("\n");
  }

  public String toXmlDocumentString(JsonValue value) throws Exception
  {
    StringWriter writer = new StringWriter(50000);
    this.setWriter(writer);
    this.startDocument();
    this.toXml(value);
    this.endDocument();
    return writer.toString();
  }

}

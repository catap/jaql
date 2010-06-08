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
package com.ibm.jaql.lang.expr.xml;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Stack;

import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.lang.StringUtils;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.IntArray;

/**
 * An expression for converting XML to JSON.
 * 
 * @see JsonToXmlFn
 */
public class XmlToJsonFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("xmlToJson", XmlToJsonFn.class);
    }
  }
  
  private XMLReader parser;
  private XmlToJsonHandler handler;

  /**
   * string xmlToJson( string xml )
   * 
   * @param exprs
   */
  public XmlToJsonFn(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public JsonValue eval(Context context) throws Exception
  {
    if( parser == null )
    {
      SAXParserFactory factory = SAXParserFactory.newInstance();
      factory.setNamespaceAware(true);
      parser = factory.newSAXParser().getXMLReader();
      handler = new XmlToJsonHandler2();
      parser.setContentHandler( handler );
    }

    JsonString s = (JsonString)exprs[0].eval(context);
    parser.parse( new InputSource(new StringReader(s.toString())) );    
    return handler.result;
  }
}

class XmlToJsonHandler extends DefaultHandler  // TODO: move, make much faster
{
  public JsonValue result;  
}

/**
 * This guy produces a record for every xml node, children and attributes
 * are in the same array as the value of the node's name.  Namespaces are
 * included in a node's record as a field called xmlns.  Text nodes have
 * the field name "text()", an attribute called "name" has a field name "@name". 
 * 
 * @author kbeyer
 */
class XmlToJsonHandler1 extends XmlToJsonHandler  // TODO: move, make much faster, add schema support
{
  public static final JsonString S_XML   = new JsonString("xml");
  public static final JsonString S_XMLNS = new JsonString("xmlns");
  public static final JsonString S_TEXT  = new JsonString("text()");
  
  protected Stack<SpilledJsonArray> stack = new Stack<SpilledJsonArray>();
  protected StringBuffer text = new StringBuffer();
  
  @Override
  public void startDocument() throws SAXException
  {
    result = null;
    stack.clear();
    stack.push(new SpilledJsonArray());
    text.setLength(0);
  }
  
  @Override
  public void endDocument() throws SAXException
  {
    SpilledJsonArray a = stack.pop();
    assert stack.empty();
    BufferedJsonRecord r = new BufferedJsonRecord();
    r.add(S_XML, a);
    result = r;
  }
  
  @Override
  public void startElement(String uri, String localName, String name, Attributes attrs)
    throws SAXException
  {
    try
    {
      SpilledJsonArray ca = new SpilledJsonArray();
      int n = attrs.getLength();
      for( int i = 0 ; i < n ; i++ )
      {
        name = "@" + attrs.getLocalName(i);
        uri = attrs.getURI(i);
        String v = attrs.getValue(i);
        BufferedJsonRecord r = new BufferedJsonRecord();
        if( uri != null && uri.length() > 0 )
        {
          r.add(S_XMLNS, new JsonString(uri));
        }
        r.add(new JsonString(name), new JsonString(v));
        ca.addCopy(r);
      }
      stack.push(ca);
    }
    catch(IOException e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }

  @Override
  public void endElement(String uri, String localName, String name)
      throws SAXException
  {
    try
    {
      endText();
      SpilledJsonArray ca = (SpilledJsonArray)stack.pop();
      BufferedJsonRecord r = new BufferedJsonRecord();
      if( uri != null && uri.length() > 0 )
      {
        r.add(new JsonString("xmlns"), new JsonString(uri));
      }
      r.add(new JsonString(localName), ca);
      SpilledJsonArray pa = (SpilledJsonArray)stack.peek();
      pa.addCopy(r);
    }
    catch(IOException e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }
  
  @Override
  public void characters(char[] ch, int start, int length) throws SAXException
  {
    text.append(ch, start, length);
  }  

  public void endText() throws IOException
  {
    if( text.length() > 0 )
    {
      BufferedJsonRecord r = new BufferedJsonRecord();
      r.add(S_TEXT, new JsonString(text.toString()));
      SpilledJsonArray pa = (SpilledJsonArray)stack.peek();
      pa.addCopy(r);
      text.setLength(0);
    }
  }  
}

/**
 * This guy places all children in a single record.  If there are multiple
 * children with the same name, an array is created to hold all the children.
 * Therefore, node order is not preserved.
 * 
 * This has an annoyance for nodes that occur once under some node and twice
 * under another node because the value is sometimes an array and sometimes
 * a single value, which makes query writing painful. This would be fixed with
 * schema support.
 *
 * Text nodes are not records, they are just strings. All text _directly_ under
 * one node is concatenated into one string value.  Therefore, mixed content is messed up! // TODO: mixed content...
 * If a node has string content and attributes or children, a field called "text()" is created
 * to store the string value.  (Again painful when it sometimes happens.)
 *  
 * Mixed-content nodes are not handled well.  Nodes are out of order and text messed up.
 * When we know it's mixed content (e.g. from schema) we could:
 *    1) make $."text()" be the concat of all text indirectly below (the string value in xquery)
 *    2) make an array [ "la", {y:["di"]}, "da", {z:["oh"]}, "my"]
 *    3) leave as xml string
 *    4) just extract string value, ignore elements below
 *    5) ...
 *
 * Schema support is also important for creating non-string values.
 * 
 * If a node is in a namespace, two records are created:
 *    { 'http://namespace': { a:'1', b:'2' }}
 * All nodes in that namespace are below the namespace field.
 * 
 * @author kbeyer
 */
class XmlToJsonHandler2 extends XmlToJsonHandler  // TODO: move, make much faster, add schema support
{
  public static final JsonString textName  = new JsonString("text()");

  /**
   * <code>LinkedList</code> is used for performance reason. Stack operations only happens at
   * one end of the list.
   */
  protected Deque<BufferedJsonRecord> stack = new LinkedList<BufferedJsonRecord>();
  /** Stores all the characters received during parsing */
  protected StringBuilder text = new StringBuilder();
  protected IntArray lengths = new IntArray();
  
  @Override
  public void startDocument() throws SAXException
  {
    result = null;
    text.setLength(0);
    stack.clear();
    stack.push(new BufferedJsonRecord());
    lengths.add(0);
  }
  
  @Override
  public void endDocument() throws SAXException
  {
    BufferedJsonRecord r = stack.pop();
    assert stack.size() == 0;
    lengths.pop();
    assert lengths.empty();
    result = r;
  }
  
  /**
   * Create a JSON record for new node. The node attributes are processed.
   */
  @Override
  public void startElement(String uri, String localName, String name, Attributes attrs)
    throws SAXException
  {
    BufferedJsonRecord node = new BufferedJsonRecord();
    stack.push(node);
    lengths.add(text.length());
    int n = attrs.getLength();
    BufferedJsonRecord container;
    for( int i = 0 ; i < n ; i++ )
    {
      uri = attrs.getURI(i);
      
      // If attribute has a uri
      if( uri != null && uri.length() > 0 )
      {
        JsonString jUri = new JsonString(uri);
        BufferedJsonRecord ur = (BufferedJsonRecord) node.get(jUri);
        if( ur == null )
        {
          ur = new BufferedJsonRecord();
          node.add(jUri, ur);
        }
        container = ur;
      } else {
        container = node;
      }
      
      String attrName = "@" + attrs.getLocalName(i);
      JsonString jName = new JsonString(attrName);
      String v = attrs.getValue(i);
      if( container.containsKey(jName) )
      {
        throw new RuntimeException("duplicate attribute name: "+attrName);
      }
      container.add(jName, new JsonString(v));
    }
  }

  /**
   * 
   */
  @Override
  public void endElement(String uri, String localName, String name)
      throws SAXException
  {
    try
    {
      BufferedJsonRecord r = stack.pop();
      int textEnd = text.length();
      int textStart = lengths.pop();
      JsonValue me = r;
      
      if( textStart < textEnd )
      {
        JsonString s = new JsonString(text.substring(textStart, textEnd));
        text.setLength(textStart);
        if( r.size() == 0 )
        {
          me = s;
        }
        else if(!StringUtils.isBlank(s.toString()))
        {
          r.add(textName, s);
        }
      }
      
      BufferedJsonRecord parent = stack.peek();
      // uri is not empty, use namespace record as parent.
      if( uri != null && uri.length() > 0 )
      {
        JsonString jUri = new JsonString(uri);
        BufferedJsonRecord ur = (BufferedJsonRecord)parent.get(jUri, null);
        if( ur == null )
        {
          ur = new BufferedJsonRecord();
          parent.add(jUri, ur);
        }
        parent = ur;
      }
      
      JsonString jLocalName = new JsonString(localName);
      if (!parent.containsKey(jLocalName))
      {
        parent.add(jLocalName, me);
      }
      else
      {
        JsonValue v = parent.get(jLocalName);
        SpilledJsonArray a;
        if( v instanceof SpilledJsonArray )
        {
          a = (SpilledJsonArray)v;
        }
        else
        {
          a = new SpilledJsonArray();
          a.addCopy(v);
          parent.set(jLocalName, a);
        }
        a.addCopy(me);
      }
    }
    catch(IOException e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }
  
  /**
   * Collects all the characters received so far.
   */
  @Override
  public void characters(char[] ch, int start, int length) throws SAXException
  {
    text.append(ch, start, length);
  } 
}

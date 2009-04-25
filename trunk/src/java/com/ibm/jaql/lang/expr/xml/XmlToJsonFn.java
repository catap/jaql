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
import java.util.Stack;

import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.type.MemoryJRecord;
import com.ibm.jaql.json.type.SpillJArray;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.util.IntArray;

/**
 * 
 */
@JaqlFn(fnName = "xmlToJson", minArgs = 1, maxArgs = 1)
public class XmlToJsonFn extends Expr
{
  protected XMLReader parser;
  protected XmlToJsonHandler handler;

  /**
   * string xmlToJson( string xml )
   * 
   * @param exprs
   */
  public XmlToJsonFn(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(Context context) throws Exception
  {
    if( parser == null )
    {
      SAXParserFactory factory = SAXParserFactory.newInstance();
      factory.setNamespaceAware(true);
      parser = factory.newSAXParser().getXMLReader();
      handler = new XmlToJsonHandler2();
      parser.setContentHandler( handler );
    }

    Item item = exprs[0].eval(context);
    JString s = (JString)item.get();
    parser.parse( new InputSource(new StringReader(s.toString())) );    
    return handler.result;
  }
}

class XmlToJsonHandler extends DefaultHandler  // TODO: move, make much faster
{
  public Item result;  
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
  public static final JString xmlName   = new JString("xml");
  public static final JString xmlnsName = new JString("xmlns");
  public static final JString textName  = new JString("text()");
  
  protected Stack<SpillJArray> stack = new Stack<SpillJArray>();
  protected StringBuffer text = new StringBuffer();
  
  @Override
  public void startDocument() throws SAXException
  {
    result = null;
    stack.clear();
    stack.push(new SpillJArray());
    text.setLength(0);
  }
  
  @Override
  public void endDocument() throws SAXException
  {
    SpillJArray a = stack.pop();
    assert stack.empty();
    MemoryJRecord r = new MemoryJRecord();
    r.add(xmlName, a);
    result = new Item(r);
  }
  
  @Override
  public void startElement(String uri, String localName, String name, Attributes attrs)
    throws SAXException
  {
    try
    {
      SpillJArray ca = new SpillJArray();
      int n = attrs.getLength();
      for( int i = 0 ; i < n ; i++ )
      {
        name = "@" + attrs.getLocalName(i);
        uri = attrs.getURI(i);
        String v = attrs.getValue(i);
        MemoryJRecord r = new MemoryJRecord();
        if( uri != null && uri.length() > 0 )
        {
          r.add(xmlnsName, new JString(uri));
        }
        r.add(name, new JString(v));
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
      SpillJArray ca = (SpillJArray)stack.pop();
      MemoryJRecord r = new MemoryJRecord();
      if( uri != null && uri.length() > 0 )
      {
        r.add("xmlns", new JString(uri));
      }
      r.add(localName, ca);
      SpillJArray pa = (SpillJArray)stack.peek();
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
      MemoryJRecord r = new MemoryJRecord();
      r.add(textName, new JString(text.toString()));
      SpillJArray pa = (SpillJArray)stack.peek();
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
 * a single value, which makes query writing painful.  This would be fixed with
 * schema support.
 *
 * Text nodes are not records, they are just strings. All text _directly_ under
 * one node is concatentated into one string value.  Therefore, mixed content is messed up! // TODO: mixed content...
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
 *    { 'http://namespace': { a:'1', b:'2' }
 * All nodes in that namespace are below the namespace field.
 * 
 * @author kbeyer
 */
class XmlToJsonHandler2 extends XmlToJsonHandler  // TODO: move, make much faster, add schema support
{
  public static final JString textName  = new JString("text()");

  protected Stack<MemoryJRecord> stack = new Stack<MemoryJRecord>();
  protected StringBuffer text = new StringBuffer();
  protected IntArray lengths = new IntArray();
  
  @Override
  public void startDocument() throws SAXException
  {
    result = null;
    text.setLength(0);
    stack.clear();
    stack.push(new MemoryJRecord());
    lengths.add(0);
  }
  
  @Override
  public void endDocument() throws SAXException
  {
    MemoryJRecord r = stack.pop();
    assert stack.empty();
    lengths.pop();
    assert lengths.empty();
    result = new Item(r);
  }
  
  @Override
  public void startElement(String uri, String localName, String name, Attributes attrs)
    throws SAXException
  {
    MemoryJRecord r = new MemoryJRecord();
    stack.push(r);
    lengths.add(text.length());
    int n = attrs.getLength();
    for( int i = 0 ; i < n ; i++ )
    {
      uri = attrs.getURI(i);
      if( uri != null && uri.length() > 0 )
      {
        Item item = r.getValue(uri, null);
        MemoryJRecord ur;
        if( item != null )
        {
          ur = (MemoryJRecord)item.get();
        }
        else
        {
          ur = new MemoryJRecord();
          item = new Item(ur);
          r.add(uri, item);
        }
        r = ur;
      }
      name = "@" + attrs.getLocalName(i);
      String v = attrs.getValue(i);
      Item item = r.getValue(name, null);
      if( item != null )
      {
        throw new RuntimeException("duplicate attribute name: "+name);
      }
      r.add(name, new JString(v));
    }
  }

  @Override
  public void endElement(String uri, String localName, String name)
      throws SAXException
  {
    try
    {
      MemoryJRecord r = stack.pop();
      int textEnd = text.length();
      int textStart = lengths.pop();
      JValue me = r;
      if( textStart < textEnd )
      {
        JString s = new JString(text.substring(textStart, textEnd));
        text.setLength(textStart);
        if( r.arity() == 0 )
        {
          me = s;
        }
        else
        {
          r.add(textName, s);
        }
      }
      MemoryJRecord parent = stack.peek();
      if( uri != null && uri.length() > 0 )
      {
        Item item = parent.getValue(uri, null);
        MemoryJRecord ur;
        if( item != null )
        {
          ur = (MemoryJRecord)item.get();
        }
        else
        {
          ur = new MemoryJRecord();
          item = new Item(ur);
          parent.add(uri, item);
        }
        parent = ur;
      }
      Item item = parent.getValue(localName, null);
      if( item == null )
      {
        parent.add(localName, me);
      }
      else
      {
        JValue v = item.get();
        SpillJArray a;
        if( v instanceof SpillJArray )
        {
          a = (SpillJArray)v;
        }
        else
        {
          a = new SpillJArray();
          a.addCopy(item);
          parent.set(localName, a);
        }
        a.addCopy(me);
      }
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
}

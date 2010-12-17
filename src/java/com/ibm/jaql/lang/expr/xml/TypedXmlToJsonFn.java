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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Deque;
import java.util.LinkedList;

import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.lang.StringUtils;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonDate;
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.IntArray;

/**
 * An expression for converting XML to JSON.
 * This function is similar to xmlToJson, except that it creates typed data, i.e., instead of producing all values as strings, 
 * 		it tries to cast each value to a closest type.
 * 
 * @see JsonToXmlFn
 */
public class TypedXmlToJsonFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("typedXmlToJson", TypedXmlToJsonFn.class);
    }
  }
  
  private XMLReader parser;
  private TypedXmlToJsonHandler handler;

  /**
   * string xmlToJson( string xml )
   * 
   * @param exprs
   */
  public TypedXmlToJsonFn(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  protected JsonValue evalRaw(Context context) throws Exception
  {
    if( parser == null )
    {
      SAXParserFactory factory = SAXParserFactory.newInstance();
      factory.setNamespaceAware(true);
      parser = factory.newSAXParser().getXMLReader();
      handler = new TypedXmlToJsonHandler2();
      parser.setContentHandler( handler );
    }

    JsonString s = (JsonString)exprs[0].eval(context);
    parser.parse(new InputSource(new StringReader(s.toString()))) ;
    return handler.result;
  }
}

class TypedXmlToJsonHandler extends DefaultHandler  // TODO: move, make much faster
{
  public JsonValue result;  
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
 */
class TypedXmlToJsonHandler2 extends TypedXmlToJsonHandler  // TODO: move, make much faster, add schema support
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
   * Instead of returning all values in the XML document as JsonString, we try to cast values to the closest data type possible.
   * Currently, we have Long, Double, Boolean, Date, String types
   */
  private JsonValue cast(String val)
  {
	  //Is it Long??
	  try {
		  long castedVal = Long.valueOf(val);
		  if (val.length() < 10)
			  return new JsonLong(castedVal);
		  else
			  return new JsonString(val);
	  } catch (NumberFormatException  e){}
	  
	  //Is it Double??
	  try {
		  double castedVal = Double.valueOf(val);
		  return new JsonDouble(castedVal);
	  } catch (NumberFormatException  e){}
	  
	  //Is it boolean
	  if (val.equals("true") || val.equals("True") || val.equals("TRUE") )
		  return JsonBool.make(true);
	  else if (val.equals("false") || val.equals("False") || val.equals("FALSE") )
		  return JsonBool.make(false);
	  
	  //Is it Date??
	  DateFormat df1 = new SimpleDateFormat("dd/MM/yyyy"); 
	  String df1Pattern = "\\d\\d/\\d\\d/\\d\\d\\d\\d";
	  DateFormat df2 = new SimpleDateFormat("dd-MM-yyyy"); 
	  String df2Pattern = "\\d\\d-\\d\\d-\\d\\d\\d\\d";
	  DateFormat df3 = new SimpleDateFormat("yyyy/MM/dd"); 
	  String df3Pattern = "\\d\\d\\d\\d/\\d\\d/\\d\\d";
	  DateFormat df4 = new SimpleDateFormat("yyyy-MM-dd"); 
	  String df4Pattern = "\\d\\d\\d\\d-\\d\\d-\\d\\d";
	  if (val.matches(df1Pattern))
	  {
		  try{
			  df1.parse(val);
			  return new JsonDate(val, df1);
		  }catch (Exception e){}
	  }
	  else if (val.matches(df2Pattern))
	  {
		  try{
			  df2.parse(val);
			  return new JsonDate(val, df2);
		  }catch (Exception e){}
	  }
	  else if (val.matches(df3Pattern))
	  {
		  try{
			  df3.parse(val);
			  return new JsonDate(val, df3);
		  }catch (Exception e){}	  
	  }
	  else if (val.matches(df4Pattern))
	  {
		  try{
			  df4.parse(val);
			  return new JsonDate(val, df4);
		  }catch (Exception e){}	
	  }		  
	  
	  //Treat the value as string
	  return new JsonString(val);
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
      container.add(jName, cast(v));
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
          me = cast(s.toString());
        }
        else if(!StringUtils.isBlank(s.toString()))
        {
          r.add(textName, cast(s.toString()));
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

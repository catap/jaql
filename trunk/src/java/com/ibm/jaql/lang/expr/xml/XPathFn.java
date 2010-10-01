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
package com.ibm.jaql.lang.expr.xml;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * Runs XPath on an XML document.
 * 
 * xpath(xml, xpath)
 */
public class XPathFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par23
  {
    public Descriptor()
    {
      super("xpath", XPathFn.class);
    }
  }
  
  protected String xpath;
  protected XPathFactory factory;
  protected XPath xpathCompiler;
  protected XPathExpression xpathExpr;
  protected MutableJsonString result;

  public XPathFn(Expr... exprs)
  {
    super(exprs);
  }

  @Override
  public JsonValue eval(Context context) throws Exception
  {
    JsonString xml = (JsonString)exprs[0].eval(context);
    if( xml == null )
    {
      return null;
    }
    JsonString jxpath = (JsonString)exprs[1].eval(context);
    if( jxpath == null )
    {
      return null;
    }
    if( xpath == null || ! xpath.equals(jxpath.toString()) )
    {
      if( factory == null )
      {
        factory = XPathFactory.newInstance();
        xpathCompiler = factory.newXPath();
        result = new MutableJsonString();
        JsonRecord namespaces = (JsonRecord)exprs[2].eval(context);
        xpathCompiler.setNamespaceContext(new RecordNamespaceContext(namespaces));
      }
      xpath = jxpath.toString();
      xpathExpr = xpathCompiler.compile(xpath);
    }
    // TODO: should this return a NodeSet?  Convert to Json? options?
    //String res = xpathExpr.evaluate(new InputSource(new StringReader(xml.toString())));
    // result.setCopy(res);
    NodeList nodeset = (NodeList)xpathExpr.evaluate(
        new InputSource(new StringReader(xml.toString())),
        XPathConstants.NODESET);
    BufferedJsonArray result = new BufferedJsonArray();
    result.clear();
    for( int i = 0 ; i < nodeset.getLength() ; i++ )
    {
      Node node = nodeset.item(i);
      result.add( toJson(node, false) );
    }
    return result;
  }
  
  public static JsonValue toJson(Node node, boolean includeNamespaces)
  {
    switch( node.getNodeType() )
    {
      case Node.ELEMENT_NODE: {
        BufferedJsonArray arr = new BufferedJsonArray();
        if( node.hasAttributes() )
        {
          NamedNodeMap attrs = node.getAttributes();
          for(int i = 0 ; i < attrs.getLength() ; i++)
          {
            Node attr = attrs.item(i);
            if( ! XMLConstants.XMLNS_ATTRIBUTE.equals( attr.getPrefix() ) )
            {
              JsonValue val = toJson(attr, includeNamespaces);
              assert val != null;
              arr.add(val);
            }
          }
        }
        String text = null;
        NodeList nodes = node.getChildNodes();
        for(int i = 0 ; i < nodes.getLength() ; i++)
        {
          Node child = nodes.item(i);
          if( child.getNodeType() == Node.ELEMENT_NODE )
          {
            JsonValue val = toJson(child, includeNamespaces);
            assert val != null;
            arr.add(val);
          }
          else
          {
            String c = child.getTextContent();
            if( c != null && c.length() > 0 )
            {
              text = (text == null) ? c : text + c;
            }
          }
        }
        JsonValue elValue;
        if( text != null && text.length() > 0 )
        {
          elValue = new JsonString(text);
          if( arr.size() > 0 )
          {
            BufferedJsonRecord textRec = new BufferedJsonRecord(1);
            textRec.add(new JsonString("text()"), elValue);
            arr.add( textRec );
            elValue = arr;
          }
        }
        else if( arr.size() > 0 )
        {
          elValue = arr;
        }
        else
        {
          elValue = JsonString.EMPTY;
        }
        BufferedJsonRecord rec = new BufferedJsonRecord(1);
        rec.add(new JsonString(node.getLocalName()), elValue);
        String uri = node.getNamespaceURI();
        if( includeNamespaces  && uri != null && uri.length() > 0 )
        {
          BufferedJsonRecord nsrec = new BufferedJsonRecord(1);
          nsrec.add(new JsonString(uri), rec);
          rec = nsrec;
        }
        return rec;
      }
      case Node.ATTRIBUTE_NODE: {
        BufferedJsonRecord rec = new BufferedJsonRecord(1);
        rec.add(new JsonString("@"+node.getLocalName()), new JsonString(node.getNodeValue()));
        String uri = node.getNamespaceURI();
        if( includeNamespaces && uri != null && uri.length() > 0 )
        {
          BufferedJsonRecord nsrec = new BufferedJsonRecord(1);
          nsrec.add(new JsonString(uri), rec);
          rec = nsrec;
        }
        return rec;
      }
      default:
        String s = node.getTextContent();
        return new JsonString(s);
    }
  }

  public static class RecordNamespaceContext implements NamespaceContext
  {
    protected HashMap<String,String> prefixToUri = new HashMap<String, String>();
    protected HashMap<String,String> uriToPrefix = new HashMap<String, String>();

    public RecordNamespaceContext(JsonRecord namespaces)
    {
      if( namespaces != null )
      {
        for( Entry<JsonString, JsonValue> e: namespaces )
        {
          prefixToUri.put(e.getKey().toString(), e.getValue().toString());
          uriToPrefix.put(e.getValue().toString(), e.getKey().toString());
        }
      }
    }
    
    @Override
    public String getNamespaceURI(String prefix)
    {
     //  if( prefix.length() == 0 ) return "bogus"; // XMLConstants.DEFAULT_NS_PREFIX;
      String uri = prefixToUri.get(prefix);
      return uri == null ? XMLConstants.NULL_NS_URI : uri;
    }

    @Override
    public String getPrefix(String uri)
    {
      return uriToPrefix.get(uri);
    }

    @Override
    public Iterator<String> getPrefixes(final String uri)
    {
      return new Iterator<String>()
      {
        String prefix = getPrefix(uri);
        
        @Override
        public boolean hasNext()
        {
          return prefix != null;
        }

        @Override
        public String next()
        {
          return prefix;
        }

        @Override
        public void remove()
        {
          throw new UnsupportedOperationException();
        }
      };
    }
  }  

}

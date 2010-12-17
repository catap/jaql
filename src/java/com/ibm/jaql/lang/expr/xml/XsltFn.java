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
import java.io.StringWriter;

import javax.xml.transform.Result;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;

import org.xml.sax.InputSource;

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * Runs XSLT on an XML document.
 * 
 * xslt(xml, xsl)
 */
public class XsltFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("xslt", XsltFn.class);
    }
  }
  
  protected String xsl;
  protected TransformerFactory factory;
  protected Transformer transformer;
  protected StringWriter strout;
  protected MutableJsonString result;

  public XsltFn(Expr... exprs)
  {
    super(exprs);
  }

  @Override
  protected JsonValue evalRaw(Context context) throws Exception
  {
    JsonString xml = (JsonString)exprs[0].eval(context);
    if( xml == null )
    {
      return null;
    }
    JsonString jxsl = (JsonString)exprs[1].eval(context);
    if( jxsl == null )
    {
      return null;
    }
    if( xsl == null || ! xsl.equals(jxsl.toString()) )
    {
      if( factory == null )
      {
        factory = TransformerFactory.newInstance();
        strout = new StringWriter();
        result = new MutableJsonString();
      }
      xsl = jxsl.toString();
      transformer = factory.newTransformer(
          new SAXSource(new InputSource(new StringReader(xsl))));
      //      for(Enumeration i = properties.propertyNames();
      //      i.hasMoreElements();) {
      //  String name = (String) i.nextElement();
      //  transformer.setParameter(name,
      //    "\'" + properties.getProperty(name) + "\'");
      // }
    }
    strout.getBuffer().setLength(0);
    XmlToJsonHandler2 handler = new XmlToJsonHandler2();
    Result tresult = new SAXResult(handler);
//    Result tresult = new StreamResult(strout);
    transformer.transform(
        new SAXSource(new InputSource(new StringReader(xml.toString()))),
        tresult);
    return handler.result;
//    result.setCopy(strout.toString());
//    return result;
  }
  
}

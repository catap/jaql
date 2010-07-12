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
package com.ibm.jaql.lang;

import java.io.Closeable;
import java.io.IOException;

import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

/**
 * Report exceptions.  
 * 
 * close() should be called.
 */
public abstract class ExceptionHandler implements Closeable
{
  /** Process an exception.
   *  
   * If this call does not throw an exception, then most callers
   * will continue  processing normally.
   * If an exception is thrown, typically by rethrowing the same error,
   * then caller will typically propagate the exception upwards to the 
   * next handler. */
  public abstract void handleException(Throwable error) throws Exception;
  
  /** Close any underlying resources of this handler. */
  @Override
  public void close() throws IOException
  {
  }
  
  
  //---------------------------------------------------------------------------------
  // Utility methods to generate JSON from Exceptions
  //---------------------------------------------------------------------------------
  
  public static final JsonString CLASS   = new JsonString("class");
  public static final JsonString STACK   = new JsonString("stack");
  public static final JsonString MESSAGE = new JsonString("message");
  public static final JsonString CAUSE   = new JsonString("cause");
  public static final JsonString FILE    = new JsonString("file");
  public static final JsonString LINE    = new JsonString("line");
  public static final JsonString METHOD  = new JsonString("method");
  public static final JsonString NATIVE  = new JsonString("native");

  public static JsonRecord makeStackTraceElement(StackTraceElement se)
  {
    BufferedJsonRecord rec = new BufferedJsonRecord();
    
    String s = se.getClassName();
    rec.add(CLASS, new JsonString(s));

    s = se.getMethodName();
    rec.add(METHOD, new JsonString(s));

    s = se.getFileName();
    if( s != null )
    {
      rec.add(FILE, new JsonString(s));
    }
    
    int n = se.getLineNumber();
    if( n > 0 )
    {
      rec.add(LINE, new JsonLong(n));
    }
    
    if( se.isNativeMethod() )
    {
      rec.add(NATIVE, JsonBool.TRUE);
    }
    
    return rec;
  }

  public static JsonArray makeStackTrace(Throwable error)
  {
//    StringWriter sw = new StringWriter();
//    PrintWriter pw = new PrintWriter(sw);
//    e.printStackTrace(pw);
//    pw.close();
//    sw.toString();
    StackTraceElement[] st = error.getStackTrace();
    JsonValue[] vals = new JsonValue[st.length];
    for(int i = 0 ; i < st.length ; i++)
    {
      vals[i] = makeStackTraceElement(st[i]);
    }
    return new BufferedJsonArray(vals,false);
  }
  
  public static JsonRecord makeExceptionRecord(Throwable error)
  {
    Throwable cause = error.getCause();
    JsonRecord causeRec = null;
    if( cause != null )
    {
      causeRec = makeExceptionRecord(cause);
    }
    BufferedJsonRecord rec = new BufferedJsonRecord();
    rec.add(CLASS, new JsonString(error.getClass().getName()));
    rec.add(STACK, makeStackTrace(error));
    String msg = error.getMessage();
    if( msg != null )
    	rec.add(MESSAGE, new JsonString(msg));
    if( causeRec != null )
    {
      rec.add(CAUSE, causeRec);
    }
    return rec;
  }
}

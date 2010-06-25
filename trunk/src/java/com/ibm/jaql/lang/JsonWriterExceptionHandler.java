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

import java.io.IOException;

import com.ibm.jaql.io.ClosableJsonWriter;
import com.ibm.jaql.json.type.JsonRecord;

/**
 * Converts the Exception to a JSON record and writes it to an error log.
 */
public class JsonWriterExceptionHandler extends ExceptionHandler
{
  protected ClosableJsonWriter writer = null;

  public JsonWriterExceptionHandler(ClosableJsonWriter writer)
  {
    this.writer = writer;
  }

  @Override
  public void handleException(Throwable e)
  {
    try
    {
      JsonRecord rec = makeExceptionRecord(e);
      writer.write(rec);
    } 
    catch(Exception ioe)
    {
      ioe.printStackTrace(System.err);
    }
  }

  @Override
  public void close() throws IOException
  {
    writer.close();
  }
}

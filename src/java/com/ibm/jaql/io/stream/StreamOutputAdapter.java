/*
 * Copyright (C) IBM Corp. 2008.
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
package com.ibm.jaql.io.stream;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;

import com.ibm.jaql.io.AbstractOutputAdapter;
import com.ibm.jaql.io.AdapterStore;
import com.ibm.jaql.io.ClosableJsonWriter;
import com.ibm.jaql.io.converter.JsonToStream;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonValue;

/** Output adapter that writes {@link Item}s to a URL, using a {@link ItemToStream} 
 * converter in the process.
 * 
 */
public class StreamOutputAdapter extends AbstractOutputAdapter
{

  protected JsonToStream<JsonValue> formatter;

  private ClosableJsonWriter     writer;

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.AbstractOutputAdapter#initializeFrom(com.ibm.jaql.json.type.JRecord)
   */
  @SuppressWarnings("unchecked")
  @Override
  public void init(JsonValue args) throws Exception
  {
    super.init(args);

    JsonRecord outputArgs = AdapterStore.getStore().output.getOption((JsonRecord)args);
    // setup the formatter
    Class<?> fclass = AdapterStore.getStore().getClassFromRecord(outputArgs,
        FORMAT_NAME, null);
    if (fclass == null) throw new Exception("formatter must be specified");
    if (!JsonToStream.class.isAssignableFrom(fclass))
      throw new Exception("formatter must implement ItemOutputStream");
    formatter = (JsonToStream) fclass.newInstance();
    JsonValue arrAcc = outputArgs.get(StreamInputAdapter.ARR_NAME);
    if(arrAcc != null) {
      formatter.setArrayAccessor( ((JsonBool)arrAcc).get());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.OutputAdapter#getItemWriter()
   */
  public ClosableJsonWriter getWriter() throws Exception
  {
    final OutputStream output = openStream(location);
    this.formatter.setOutputStream(output);
    this.writer = new ClosableJsonWriter() {
      
      public void close() throws IOException
      {
        formatter.close();
      }

      public void write(JsonValue value) throws IOException
      {
        formatter.write(value);
      }

    };
    return writer;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.AbstractOutputAdapter#close()
   */
  @Override
  public void close() throws Exception
  {
    if (writer != null) writer.close();
  }

  /**
   * @param location
   * @return
   * @throws Exception
   */
  protected OutputStream openStream(String location) throws Exception
  {
    // make a URI from location.
    URI uri = new URI(location);
    URL url = uri.toURL();
    URLConnection c = url.openConnection();
    return c.getOutputStream();
  }
}

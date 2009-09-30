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
import com.ibm.jaql.io.stream.converter.JsonToStreamConverter;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonValue;

/**
 * Output adapter that writes {@link Item}s to a URL, using a
 * {@link ItemToStream} converter in the process.
 */
public class StreamOutputAdapter extends AbstractOutputAdapter {

  private JsonToStream<JsonValue> formatter;
  private ClosableJsonWriter writer;
  private JsonToStreamConverter converter;

  @SuppressWarnings("unchecked")
  @Override
  public void init(JsonValue args) throws Exception {
    super.init(args);
    AdapterStore as = AdapterStore.getStore();
    JsonRecord outputArgs = as.output.getOption((JsonRecord) args);

    // formatter
    Class<JsonToStream> fclass = (Class<JsonToStream>) as.getClassFromRecord(outputArgs,
                                                                             FORMAT_NAME,
                                                                             null);
    if (fclass == null)
      throw new IllegalArgumentException("formatter must be specified");
    formatter = fclass.newInstance();
    JsonValue arrAcc = outputArgs.get(StreamInputAdapter.ARR_NAME);
    if (arrAcc != null) {
      formatter.setArrayAccessor(((JsonBool) arrAcc).get());
    }

    // converter
    Class<JsonToStreamConverter> converterClass = (Class<JsonToStreamConverter>) as.getClassFromRecord(options,
                                                                                 CONVERTER_NAME,
                                                                                 null);
    if (converterClass != null) {
      this.converter = converterClass.newInstance();
      this.converter.init(options);
    }
  }

  @Override
  public ClosableJsonWriter getWriter() throws Exception {
    final OutputStream output = openStream(location);
    formatter.setOutputStream(output);
    writer = new ClosableJsonWriter() {
      @Override
      public void close() throws IOException {
        formatter.cleanUp();
      }

      @Override
      public void write(JsonValue value) throws IOException {
        JsonValue v = converter == null ? value : converter.convert(value);
        formatter.write(v);
      }
    };
    return writer;
  }

  @Override
  public void close() throws Exception {
    if (writer != null)
      writer.close();
  }

  /**
   * Opens the output stream to the given location. STDOUT is returned if the
   * location is <code>null</code>.
   * 
   * @param location URL string
   * @return An ouput stream
   * @throws Exception
   */
  protected OutputStream openStream(String location) throws Exception {
    if (location == null) {
      return System.out;
    } else {
      // make a URI from location.
      URI uri = new URI(location);
      URL url = uri.toURL();
      URLConnection c = url.openConnection();
      return c.getOutputStream();
    }
  }
}

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

import java.io.OutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;

import com.ibm.jaql.io.AbstractOutputAdapter;
import com.ibm.jaql.io.AdapterStore;
import com.ibm.jaql.io.ItemWriter;
import com.ibm.jaql.io.converter.ItemToStream;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JRecord;

/**
 * 
 */
public class StreamOutputAdapter extends AbstractOutputAdapter
{

  protected ItemToStream formatter;

  private ItemWriter     writer;

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.AbstractOutputAdapter#initializeFrom(com.ibm.jaql.json.type.JRecord)
   */
  @Override
  protected void initializeFrom(JRecord args) throws Exception
  {
    super.initializeFrom(args);

    JRecord outputArgs = AdapterStore.getStore().output.getOption(args);
    // setup the formatter
    Class<?> fclass = AdapterStore.getStore().getClassFromRecord(outputArgs,
        FORMAT_NAME, null);
    if (fclass == null) throw new Exception("formatter must be specified");
    if (!ItemToStream.class.isAssignableFrom(fclass))
      throw new Exception("formatter must implement ItemOutputStream");
    formatter = (ItemToStream) fclass.newInstance();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.OutputAdapter#getItemWriter()
   */
  public ItemWriter getItemWriter() throws Exception
  {
    final OutputStream output = openStream(location);
    this.formatter.setOutputStream(output);
    this.writer = new ItemWriter() {

      public void close() throws IOException
      {
        formatter.close();
      }

      public void write(Item value) throws IOException
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

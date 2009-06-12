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

import java.io.InputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;

import com.ibm.jaql.io.AbstractInputAdapter;
import com.ibm.jaql.io.AdapterStore;
import com.ibm.jaql.io.ItemReader;
import com.ibm.jaql.io.converter.StreamToItem;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JBool;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;

/** Input adapter that reads from data from a URL (constructed using the provided location
 * and all arguments) and converts the data to items using a {@link StreamToItem} converter.
 * 
 * <p> Usage: stRead(location: 'uri', {adapter: 'StreamInputAdapter', format:
 * 'ItemInputStream', args: { } });
 */
public class StreamInputAdapter extends AbstractInputAdapter
{

  public static String   ARGS_NAME = "args";
  public static String   ARR_NAME  = "asArray"; // @see com.ibm.jaql.io.converter.StreamToItem

  protected StreamToItem formatter;

  protected JRecord      strArgs;

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.AbstractInputAdapter#initializeFrom(com.ibm.jaql.json.type.JRecord)
   */
  @Override
  protected void initializeFrom(JRecord args) throws Exception
  {
    super.initializeFrom(args);

    JRecord inputArgs = AdapterStore.getStore().input.getOption(args);
    
    // setup the formatter
    Class<?> fclass = AdapterStore.getStore().getClassFromRecord(inputArgs,
        FORMAT_NAME, null);
    if (fclass == null) throw new Exception("formatter must be specified");
    if (!StreamToItem.class.isAssignableFrom(fclass))
      throw new Exception("formatter must implement ItemInputStream");
    formatter = (StreamToItem) fclass.newInstance();
    Item arrAcc = inputArgs.getValue(ARR_NAME);
    if(!arrAcc.isNull()) {
      formatter.setArrayAccessor( ((JBool)arrAcc.get()).value);
    }

    // setup the args
    Item item = inputArgs.getValue(ARGS_NAME);
    if (!item.isNull()) strArgs = (JRecord) item.get();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.InputAdapter#getItemReader()
   */
  public ItemReader getItemReader() throws Exception
  {

    final InputStream istr = openStream(location, strArgs);
    formatter.setInputStream(istr);

    return new ItemReader() {

      @Override
      public void close() throws IOException
      {
        istr.close();
      }

      @Override
      public boolean next(Item value) throws IOException
      {
        return formatter.read(value);
      }

      @Override
      public Item createValue()
      {
        return formatter.createTarget();
      }
    };
  }

  /**
   * @param location
   * @param args
   * @return
   * @throws Exception
   */
  protected InputStream openStream(String location, JRecord args)
      throws Exception
  {
    // make a URI from location. args are converted to the query component
    String uriStr = location;
    // TODO: memory!!
    if (args != null)
    {
      String sep = "?";
      for (int i = 0; i < args.arity(); i++)
      {
        JString name = args.getName(i);
        Item value = args.getValue(i);
        JValue w = value.get();
        if (w != null)
        {
          String s = w.toString();
          // System.out.println(name + "=" + s);
          s = URLEncoder.encode(s, "UTF-8");
          uriStr += sep + name + "=" + s;
          sep = "&";
        }
      }
    }
    URI uri = new URI(uriStr);
    URL url = uri.toURL();
    return url.openStream();
  }
}

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
import java.util.Map.Entry;

import com.ibm.jaql.io.AbstractInputAdapter;
import com.ibm.jaql.io.AdapterStore;
import com.ibm.jaql.io.ClosableJsonIterator;
import com.ibm.jaql.io.converter.StreamToJson;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

/** Input adapter that reads from data from a URL (constructed using the provided location
 * and all arguments) and converts the data to items using a {@link StreamToItem} converter.
 * 
 * <p> Usage: stRead(location: 'uri', {adapter: 'StreamInputAdapter', format:
 * 'ItemInputStream', args: { } });
 */
public class StreamInputAdapter extends AbstractInputAdapter
{

  public static final JsonString   ARGS_NAME = new JsonString("args");
  public static final JsonString   ARR_NAME  = new JsonString("asArray"); // @see com.ibm.jaql.io.converter.StreamToItem

  protected StreamToJson<JsonValue> formatter;

  protected JsonRecord      strArgs;

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.AbstractInputAdapter#initializeFrom(com.ibm.jaql.json.type.JRecord)
   */
  @SuppressWarnings("unchecked")
  @Override
  public void init(JsonValue args) throws Exception
  {
    super.init(args);

    JsonRecord inputArgs = AdapterStore.getStore().input.getOption((JsonRecord)args);
    
    // setup the formatter
    Class<?> fclass = AdapterStore.getStore().getClassFromRecord(inputArgs,
        FORMAT_NAME, null);
    if (fclass == null) throw new Exception("formatter must be specified");
    if (!StreamToJson.class.isAssignableFrom(fclass))
      throw new Exception("formatter must implement ItemInputStream");
    formatter = (StreamToJson<JsonValue>) fclass.newInstance();
    JsonValue arrAcc = inputArgs.get(ARR_NAME);
    if(arrAcc != null) {
      formatter.setArrayAccessor( ((JsonBool)arrAcc).get());
    }

    // setup the args
    JsonValue value = inputArgs.get(ARGS_NAME);
    if (value != null) strArgs = (JsonRecord) value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.InputAdapter#getItemReader()
   */
  public ClosableJsonIterator iter() throws Exception
  {

    final InputStream istr = openStream(location, strArgs);
    formatter.setInput(istr);

    return new ClosableJsonIterator() { // TODO: temporary hack until interfaces are adapted
//      boolean first = true;
      JsonValue val = null; 
      
      @Override
      public void close() throws IOException
      {
        istr.close();
      }

      @Override
      public boolean moveNext() throws IOException
      {
        val = formatter.read(val);
        if(val == null)
          return false;
        currentValue = val;
        return true;
      }
    };
  }

  public Schema getSchema()
  {
    return new ArraySchema(null, formatter.getSchema());
  }
  
  /**
   * @param location
   * @param args
   * @return
   * @throws Exception
   */
  protected InputStream openStream(String location, JsonRecord args)
      throws Exception
  {
    // make a URI from location. args are converted to the query component
    String uriStr = location;
    // TODO: memory!!
    if (args != null)
    {
      String sep = "?";
      for (Entry<JsonString, JsonValue> e : args)
      {
        JsonString name = e.getKey();
        JsonValue w = e.getValue();        
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

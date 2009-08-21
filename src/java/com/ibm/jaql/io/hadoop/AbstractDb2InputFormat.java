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
package com.ibm.jaql.io.hadoop;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.UndeclaredThrowableException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.parser.ParseException;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;


public abstract class AbstractDb2InputFormat implements InputFormat<JsonHolder, JsonHolder>
{
  public static final String DRIVER_KEY              = "com.ibm.db2.input.driver";
  public static final String URL_KEY                 = "com.ibm.db2.input.url";
  public static final String PROPERTIES_KEY          = "com.ibm.db2.input.properties"; // json record key/value into JDBC Properties

  protected Driver driver;
  protected Properties props;
  protected Connection conn;


  protected void init(JobConf conf, Properties overrides) throws IOException, SQLException
  {
    String url        = conf.get(URL_KEY);
    String propRec    = conf.get(PROPERTIES_KEY);
    Class<? extends Driver> driverClass = 
      conf.getClass(DRIVER_KEY, Driver.class).asSubclass(Driver.class);

    try 
    {
      driver = driverClass.newInstance();
    }
    catch (Exception e)
    {
      throw new UndeclaredThrowableException(e);// IOException("Error constructing jdbc driver", e);
    }

    props = new Properties();
    
    if( propRec != null && ! "".equals(propRec))
    {
      try
      {
        JsonParser parser = new JsonParser(new StringReader(propRec));
        JsonRecord jrec = (JsonRecord)parser.JsonVal();
        for (Entry<JsonString, JsonValue> f : jrec)
        {
          JsonString key = f.getKey();
          JsonValue value = f.getValue();
          props.setProperty(key.toString(), value.toString());
        }
      }
      catch(ParseException pe)
      {
        throw new UndeclaredThrowableException(pe); // IOException("couldn't parse "+PROPERTIES_KEY+" = "+jsonRec, pe);
      }
    }
    // props.put("readOnly", true);
    if( overrides != null )
    {
      props.putAll(overrides);
    }

//    DriverPropertyInfo[] info = driver.getPropertyInfo(url, props);

    conn = driver.connect(url, props);
  }
  
  protected void init(JobConf conf) throws IOException, SQLException
  {
    this.init(conf, null);
  }


  
  @Deprecated
  public void validateInput(JobConf conf) throws IOException
  {
  }
}

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
package com.ibm.jaql.io.registry;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;

import com.ibm.jaql.io.converter.FromJson;
import com.ibm.jaql.io.converter.ToJson;
import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.util.FastPrintStream;

/** Super class for registry formats that serialize the (key, value)-pairs in the registry
 * to {@link JsonRecord}s. Subclasses provide methods that convert keys/values to {@link Items}s
 * and vice versa.
 * 
 * @param <K>
 * @param <V>
 */
public abstract class JsonRegistryFormat<K, V>
    implements RegistryFormat<K, V, JsonRecord>
{

  public static final JsonString KEY_NAME = new JsonString("key");

  public static final JsonString VAL_NAME = new JsonString("val");

  private ToJson<K>        toKeyConverter;

  private ToJson<V>        toValConverter;

  private FromJson<K>          fromKeyConverter;

  private FromJson<V>          fromValConverter;

  /**
   * 
   */
  public JsonRegistryFormat()
  {
    toKeyConverter = createToKeyConverter();
    toValConverter = createToValConverter();
    fromKeyConverter = createFromKeyConverter();
    fromValConverter = createFromValConverter();
  }

  /**
   * @return
   */
  protected abstract ToJson<K> createToKeyConverter();

  /**
   * @return
   */
  protected abstract ToJson<V> createToValConverter();

  /**
   * @return
   */
  protected abstract FromJson<K> createFromKeyConverter();

  /**
   * @return
   */
  protected abstract FromJson<V> createFromValConverter();

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.registry.RegistryFormat#convert(java.lang.Object,
   *      java.lang.Object)
   */
  public BufferedJsonRecord convert(K key, V value)
  {
    BufferedJsonRecord r = new BufferedJsonRecord();
    JsonValue kTgt = toKeyConverter.createTarget();
    JsonValue vTgt = toValConverter.createTarget();
    kTgt = toKeyConverter.convert(key, kTgt);
    vTgt = toValConverter.convert(value, vTgt);
    r.add(KEY_NAME, kTgt);
    r.add(VAL_NAME, vTgt);
    return r;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.registry.RegistryFormat#convertKey(com.ibm.jaql.json.type.JValue)
   */
  public K convertKey(JsonRecord external)
  {
    JsonValue kValue = external.get(KEY_NAME);
    K kTgt = fromKeyConverter.createTarget();
    kTgt = fromKeyConverter.convert(kValue, kTgt);
    return kTgt;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.registry.RegistryFormat#convertVal(com.ibm.jaql.json.type.JValue)
   */
  public V convertVal(JsonRecord external)
  {
    JsonValue vValue= external.get(VAL_NAME);
    V vTgt = fromValConverter.createTarget();
    vTgt = fromValConverter.convert(vValue, vTgt);
    return vTgt;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.registry.RegistryFormat#readRegistry(java.io.InputStream,
   *      com.ibm.jaql.io.registry.Registry)
   */
  public void readRegistry(InputStream input, Registry<K, V> registry)
      throws Exception
  {
    // Array of records
    JsonParser parser = new JsonParser(input);
    JsonArray arr = (JsonArray) parser.TopVal();
    long n = arr.count();
    for (long i = 0; i < n; i++)
    {
      JsonRecord r = (JsonRecord) arr.get(i);
      K kVal = convertKey(r);
      V vVal = convertVal(r);
      registry.register(kVal, vVal);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.registry.RegistryFormat#writeRegistry(java.io.OutputStream,
   *      java.util.Iterator)
   */
  public void writeRegistry(OutputStream out, Iterator<Map.Entry<K, V>> iter)
      throws Exception
  {
    // Array of records
    FastPrintStream pout = new FastPrintStream(out, FastPrintStream.UTF8);
    SpilledJsonArray arr = new SpilledJsonArray();
    while (iter.hasNext())
    {
      Map.Entry<K, V> entry = iter.next();
      JsonRecord r = convert(entry.getKey(), entry.getValue());
      arr.addCopy(r);
    }
    JsonUtil.print(pout, arr);
    pout.flush();
  }

}

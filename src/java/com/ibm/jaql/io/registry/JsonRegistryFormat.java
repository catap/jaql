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
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Map;

import com.ibm.jaql.io.converter.*;
import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.MemoryJRecord;
import com.ibm.jaql.json.type.SpillJArray;

/** Super class for registry formats that serialize the (key, value)-pairs in the registry
 * to {@link JRecord}s. Subclasses provide methods that convert keys/values to {@link Items}s
 * and vice versa.
 * 
 * @param <K>
 * @param <V>
 */
public abstract class JsonRegistryFormat<K, V>
    implements RegistryFormat<K, V, JRecord>
{

  public static final String KEY_NAME = "key";

  public static final String VAL_NAME = "val";

  private ToItem<K>        toKeyConverter;

  private ToItem<V>        toValConverter;

  private FromItem<K>          fromKeyConverter;

  private FromItem<V>          fromValConverter;

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
  protected abstract ToItem<K> createToKeyConverter();

  /**
   * @return
   */
  protected abstract ToItem<V> createToValConverter();

  /**
   * @return
   */
  protected abstract FromItem<K> createFromKeyConverter();

  /**
   * @return
   */
  protected abstract FromItem<V> createFromValConverter();

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.registry.RegistryFormat#convert(java.lang.Object,
   *      java.lang.Object)
   */
  public MemoryJRecord convert(K key, V value)
  {
    MemoryJRecord r = new MemoryJRecord();
    Item kTgt = toKeyConverter.createTarget();
    Item vTgt = toValConverter.createTarget();
    toKeyConverter.convert(key, kTgt);
    toValConverter.convert(value, vTgt);
    r.add(KEY_NAME, kTgt);
    r.add(VAL_NAME, vTgt);
    return r;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.registry.RegistryFormat#convertKey(com.ibm.jaql.json.type.JValue)
   */
  public K convertKey(JRecord external)
  {
    Item kItem = external.getValue(KEY_NAME);
    K kTgt = fromKeyConverter.createTarget();
    fromKeyConverter.convert(kItem, kTgt);
    return kTgt;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.registry.RegistryFormat#convertVal(com.ibm.jaql.json.type.JValue)
   */
  public V convertVal(JRecord external)
  {
    Item vItem = external.getValue(VAL_NAME);
    V vTgt = fromValConverter.createTarget();
    fromValConverter.convert(vItem, vTgt);
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
    JArray arr = (JArray) parser.TopVal().get();
    long n = arr.count();
    for (long i = 0; i < n; i++)
    {
      JRecord r = (JRecord) arr.nth(i).get();
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
    PrintStream pout = new PrintStream(out);
    SpillJArray arr = new SpillJArray();
    while (iter.hasNext())
    {
      Map.Entry<K, V> entry = iter.next();
      JRecord r = convert(entry.getKey(), entry.getValue());
      arr.addCopy(r);
    }
    arr.print(pout);
    pout.flush();
  }

}

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
package com.ibm.jaql.io.hadoop.converter;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.ibm.jaql.io.converter.FromJson;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonValue;

/** 
 * Base class for converters that convert an {@link Item} into a Hadoop record, i.e., 
 * a (key, value)-pair where the key is of type {@link WritableComparable} and the 
 * value is of type {@link Writable}. 
 */
public abstract class JsonToHadoopRecord<K,V> implements KeyValueExport<K, V>
{

  protected FromJson<K> keyConverter;

  protected FromJson<V> valueConverter;

  protected Converter converter;

  protected abstract FromJson<K> createKeyConverter();

  protected abstract FromJson<V> createValueConverter();

  /**
   * If a key converter has been specified, use its target. Otherwise use null.
   * @return
   */
  public K createKeyTarget()
  {
    if (keyConverter == null) return null;
    return keyConverter.createInitialTarget();
  }

  /**
   * If a val converter has been specified, use its target. Otherwise use null.
   * @return
   */
  public V createValueTarget()
  {
    if (valueConverter == null) return null;
    return valueConverter.createInitialTarget();
  }

  /**
   * 
   */
  public JsonToHadoopRecord()
  {
    keyConverter = createKeyConverter();
    valueConverter = createValueConverter();
    if (keyConverter != null && valueConverter == null)
      converter = new KeyConverter();
    else if (keyConverter == null && valueConverter != null)
      converter = new ValueConverter();
    else if (keyConverter != null && valueConverter != null)
      converter = new PairConverter();
    else
      throw new RuntimeException("at least one converter must be specified");
  }

  /**
   * @param src
   * @param key
   * @param value
   */
  public void convert(JsonValue src, K key, V value)
  {
    converter.convert(src, key, value);
  }

  /**
   * 
   * @param options
   */
  public void init(JsonRecord options)
  {
    // do nothing
  }
  
  /**
   * 
   */
  protected abstract class Converter
  {
    abstract void convert(JsonValue src, K key, V value);
  }

  /**
   * 
   */
  protected final class KeyConverter extends Converter
  {
    void convert(JsonValue src, K key, V value)
    {
      K newKey = keyConverter.convert(src, key);
      assert key==newKey; // reference check intended
    }
  }

  /**
   * 
   */
  protected final class ValueConverter extends Converter
  {
    void convert(JsonValue src, K key, V value)
    {
      V newValue = valueConverter.convert(src, value);
      assert value==newValue; // reference check intended
    }
  }

  /**
   * 
   */
  protected final class PairConverter extends Converter
  {
    void convert(JsonValue src, K key, V value)
    {
      K newKey = keyConverter.convert(src, key);
      assert key==newKey; // reference check intended
      V newValue = valueConverter.convert(src, value);
      assert value==newValue; // reference check intended
    }
  }
}

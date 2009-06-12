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

import com.ibm.jaql.io.converter.ToJson;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonValue;

/** 
 * Concrete class for converters that convert a Hadoop record---i.e., 
 * a (key, value)-pair where the key is of type {@link WritableComparable} and the 
 * value is of type {@link Writable}---into an {@link Item}.
 */
public abstract class HadoopRecordToJson<K, V> implements KeyValueImport<K, V>//<WritableComparable, Writable>
{
  /**
   * 
   */
  protected ToJson<K> keyConverter;
  
  /**
   * 
   */
  protected ToJson<V> valueConverter;

  /**
   * 
   */
  protected Converter converter;

  /**
   * 
   */
  public HadoopRecordToJson()
  {
    keyConverter = createKeyConverter();
    valueConverter = createValueConverter();
    if (keyConverter != null && valueConverter == null)
      converter = new KeyConverter();
    else if (keyConverter == null && valueConverter != null)
      converter = new ValueConverter();
    else if (keyConverter != null && valueConverter != null)
      converter = createPairConverter();
    else
      throw new RuntimeException("at least one converter must be specified");
  }

  /* 
   * Default implementation is to do nothing.
   * 
   * (non-Javadoc)
   * @see com.ibm.jaql.io.hadoop.converter.KeyValueImport#init(com.ibm.jaql.json.type.JRecord)
   */
  public void init(JsonRecord options)
  {
    // do nothing
  }
  
  /* (non-Javadoc)
   * @see com.ibm.jaql.io.hadoop.converter.KeyValueImport#createTarget()
   */
  public JsonValue createInitialTarget()
  {
    return converter.createTarget();
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.io.hadoop.converter.KeyValueImport#convert(java.lang.Object, java.lang.Object, com.ibm.jaql.json.type.Item)
   */
  public final JsonValue convert(K key, V val, JsonValue target)
  {
    return converter.convert(key, val, target);
  }

  /**
   * @return
   */
  protected abstract ToJson<K> createKeyConverter();

  /**
   * @return
   */
  protected abstract ToJson<V> createValueConverter();
  
  /**
   * @return
   */
  protected Converter createPairConverter() {
    return new PairConverter();
  }

  /**
   * 
   */
  public abstract class Converter
  {
    /**
     * @param key
     * @param value
     * @param tgt
     */
    public abstract JsonValue convert(K key, V value, JsonValue target);
    
    /**
     * @return
     */
    public abstract JsonValue createTarget();
  }

  /**
   * 
   */
  public final class KeyConverter extends Converter
  {
	/* (non-Javadoc)
	 * @see com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem.Converter#convert(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.Writable, com.ibm.jaql.json.type.Item)
	 */
	public JsonValue convert(K key, V val, JsonValue target)
    {
      return keyConverter.convert(key, target);
    }
	
	/* (non-Javadoc)
	 * @see com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem.Converter#createTarget()
	 */
	public JsonValue createTarget() {
	  return keyConverter.createInitialTarget();
	}
  }

  /**
   * 
   */
  public final class ValueConverter extends Converter
  {
    /* (non-Javadoc)
     * @see com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem.Converter#convert(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.Writable, com.ibm.jaql.json.type.Item)
     */
    public JsonValue convert(K key, V value, JsonValue target)
    {
      return valueConverter.convert(value, target);
    }
	
    /* (non-Javadoc)
     * @see com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem.Converter#createTarget()
     */
    public JsonValue createTarget() {
      return valueConverter.createInitialTarget();
    }
  }

  /**
   * 
   */
  public class PairConverter extends Converter
  {
    /* (non-Javadoc)
	   * @see com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem.Converter#convert(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.Writable, com.ibm.jaql.json.type.Item)
	   */
    public JsonValue convert(K key, V value, JsonValue target)
    {
      BufferedJsonArray array = (BufferedJsonArray) target;
      try
      {
        array.set(0, keyConverter.convert(key, array.nth(0)));
        array.set(1, valueConverter.convert(value, array.nth(1)));
        return array;
      }
      catch (Exception e)
      {
        throw new RuntimeException(e);
      }
    }
  
    /* (non-Javadoc)
     * @see com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem.Converter#createTarget()
     */
    public JsonValue createTarget() 
    {
      BufferedJsonArray array = new BufferedJsonArray(2);
      array.set(0, keyConverter.createInitialTarget());
      array.set(1, valueConverter.createInitialTarget());
      return array;
    }
  }
}

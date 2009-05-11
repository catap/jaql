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

import com.ibm.jaql.io.converter.ToItem;
import com.ibm.jaql.json.type.FixedJArray;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JRecord;

/** 
 * Concrete class for converters that convert a Hadoop record---i.e., 
 * a (key, value)-pair where the key is of type {@link WritableComparable} and the 
 * value is of type {@link Writable}---into an {@link Item}.
 */
public abstract class HadoopRecordToItem<K, V> implements KeyValueImport<K, V>//<WritableComparable, Writable>
{
  /**
   * 
   */
  protected ToItem<K> keyConverter;
  
  /**
   * 
   */
  protected ToItem<V>           valConverter;

  /**
   * 
   */
  protected Converter                    converter;

  /**
   * 
   */
  public HadoopRecordToItem()
  {
    keyConverter = createKeyConverter();
    valConverter = createValConverter();
    if (keyConverter != null && valConverter == null)
      converter = new KeyConverter();
    else if (keyConverter == null && valConverter != null)
      converter = new ValConverter();
    else if (keyConverter != null && valConverter != null)
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
  public void init(JRecord options)
  {
    // do nothing
  }
  
  /* (non-Javadoc)
   * @see com.ibm.jaql.io.hadoop.converter.KeyValueImport#createTarget()
   */
  public Item createTarget()
  {
    return converter.createTarget();
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.io.hadoop.converter.KeyValueImport#convert(java.lang.Object, java.lang.Object, com.ibm.jaql.json.type.Item)
   */
  public final void convert(K key, V val, Item tgt)
  {
    converter.convert(key, val, tgt);
  }

  /**
   * @return
   */
  protected abstract ToItem<K> createKeyConverter();

  /**
   * @return
   */
  protected abstract ToItem<V> createValConverter();
  
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
     * @param val
     * @param tgt
     */
    public abstract void convert(K key, V val, Item tgt);
    
    /**
     * @return
     */
    public abstract Item createTarget();
  }

  /**
   * 
   */
  public final class KeyConverter extends Converter
  {
	/* (non-Javadoc)
	 * @see com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem.Converter#convert(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.Writable, com.ibm.jaql.json.type.Item)
	 */
	public void convert(K key, V val, Item tgt)
    {
      keyConverter.convert(key, tgt);
    }
	
	/* (non-Javadoc)
	 * @see com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem.Converter#createTarget()
	 */
	public Item createTarget() {
	  return keyConverter.createTarget();
	}
  }

  /**
   * 
   */
  public final class ValConverter extends Converter
  {
	/* (non-Javadoc)
	 * @see com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem.Converter#convert(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.Writable, com.ibm.jaql.json.type.Item)
	 */
	public void convert(K key, V val, Item tgt)
    {
      valConverter.convert(val, tgt);
    }
	
	/* (non-Javadoc)
	 * @see com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem.Converter#createTarget()
	 */
	public Item createTarget() {
	  return valConverter.createTarget();
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
	public void convert(K key, V val, Item tgt)
    {
      JArray arr = (JArray) tgt.get();
      try
      {
        keyConverter.convert(key, arr.nth(0));
        valConverter.convert(val, arr.nth(1));
      }
      catch (Exception e)
      {
        throw new RuntimeException(e);
      }
    }
	
	/* (non-Javadoc)
	 * @see com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem.Converter#createTarget()
	 */
	public Item createTarget() {
	  JArray arr = new FixedJArray(2);
	  return new Item(arr);
	}
  }
}

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

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JRecord;

/**
 * 
 */
public abstract class ItemToHadoopRecord
{

  protected ItemToWritableComparable keyConverter;

  protected ItemToWritable           valConverter;

  protected Converter                converter;

  protected abstract ItemToWritableComparable createKeyConverter();

  protected abstract ItemToWritable createValConverter();

  /**
   * @return
   */
  public WritableComparable createKeyTarget()
  {
    if (keyConverter == null) return null;
    return keyConverter.createTarget();
  }

  /**
   * @return
   */
  public Writable createValTarget()
  {
    if (valConverter == null) return null;
    return valConverter.createTarget();
  }

  /**
   * 
   */
  public ItemToHadoopRecord()
  {
    keyConverter = createKeyConverter();
    valConverter = createValConverter();
    if (keyConverter != null && valConverter == null)
      converter = new KeyConverter();
    else if (keyConverter == null && valConverter != null)
      converter = new ValConverter();
    else if (keyConverter != null && valConverter != null)
      converter = new PairConverter();
    else
      throw new RuntimeException("at least one converter must be specified");
  }

  /**
   * @param src
   * @param key
   * @param val
   */
  public void convert(Item src, WritableComparable key, Writable val)
  {
    converter.convert(src, key, val);
  }

  /**
   * 
   * @param options
   */
  public void init(JRecord options)
  {
    // do nothing
  }
  
  /**
   * 
   */
  protected abstract class Converter
  {
    abstract void convert(Item src, WritableComparable key, Writable val);
  }

  /**
   * 
   */
  protected final class KeyConverter extends Converter
  {
    void convert(Item src, WritableComparable key, Writable val)
    {
      keyConverter.convert(src, key);
    }
  }

  /**
   * 
   */
  protected final class ValConverter extends Converter
  {
    void convert(Item src, WritableComparable key, Writable val)
    {
      valConverter.convert(src, val);
    }
  }

  /**
   * 
   */
  protected final class PairConverter extends Converter
  {
    void convert(Item src, WritableComparable key, Writable val)
    {
      keyConverter.convert(src, key);
      valConverter.convert(src, val);
    }
  }
}

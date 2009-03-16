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
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JRecord;

/**
 * 
 */
public abstract class HadoopRecordToItem
{

  protected WritableComparableToItem keyConverter;

  protected WritableToItem           valConverter;

  protected Converter                converter;

  protected abstract WritableComparableToItem createKeyConverter();

  protected abstract WritableToItem createValConverter();

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
      converter = new PairConverter();
    else
      throw new RuntimeException("at least one converter must be specified");
  }

  /**
   * @return
   */
  public Item createTarget()
  {
    return new Item();
  }

  /**
   * @param key
   * @param val
   * @param tgt
   */
  public final void convert(WritableComparable key, Writable val, Item tgt)
  {
    converter.convert(key, val, tgt);
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
    /**
     * @param key
     * @param val
     * @param tgt
     */
    abstract void convert(WritableComparable key, Writable val, Item tgt);
  }

  /**
   * 
   */
  protected final class KeyConverter extends Converter
  {
    void convert(WritableComparable key, Writable val, Item tgt)
    {
      keyConverter.convert(key, tgt);
    }
  }

  /**
   * 
   */
  protected final class ValConverter extends Converter
  {
    void convert(WritableComparable key, Writable val, Item tgt)
    {
      valConverter.convert(val, tgt);
    }
  }

  /**
   * 
   */
  protected final class PairConverter extends Converter
  {
    void convert(WritableComparable key, Writable val, Item tgt)
    {
      // assume that tgt is a JArray of length 2
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
  }
}

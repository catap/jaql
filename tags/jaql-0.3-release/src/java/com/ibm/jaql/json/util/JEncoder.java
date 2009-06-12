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
package com.ibm.jaql.json.util;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JAtom;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;

/**
 * 
 */
public abstract class JEncoder
{
  /**
   * @param item
   */
  public abstract void write(Item item);

  /**
   * @param options
   */
  public abstract void setOptions(JRecord options);

  /**
   * @param atom
   */
  public abstract void writeAtom(JAtom atom);

  /**
   * @param array
   */
  public abstract void writeArrayBegin(JArray array);

  /**
   * 
   */
  public abstract void writeArrayEnd();

  /**
   * @param record
   */
  public abstract void writeRecordBegin(JRecord record);

  /**
   * 
   */
  public abstract void writeRecordEnd();

  /**
   * @param name
   */
  public abstract void writeFieldBegin(JString name);

  /**
   * 
   */
  public abstract void writeFieldEnd();

  /**
   * 
   */
  public abstract void close();
}

abstract class JDecoder
{
  public static enum DecodeType
  {
    EOF, NULL, ATOM, ARRAY, END_ARRAY, RECORD, END_RECORD, FIELD_NAME,
  };

  public abstract JRecord getOptions();

  public abstract DecodeType next();

  /**
   * Get the current atom. Only valid when next() just returned ATOM.
   * 
   * @return
   */
  public abstract JAtom getAtom();

  /**
   * Get the current field name. Only valid when next() just returned
   * FIELD_NAME.
   * 
   * @return
   */
  public abstract JString getName();

  public abstract void close();
}

abstract class JInput
{
  public abstract JRecord getOptions();
  public abstract JDecoder getDecoder();
  public abstract Item.Encoding getEncoding(int encodingId);
  public abstract JString getString(int stringId);
}

abstract class JOutput
{
  public abstract void setOptions(JRecord options);
  public abstract void setDecoder(JDecoder decoder);
  public abstract int getEncodingId(Item.Encoding encoding);
  public abstract long getStringId(JString string);
}

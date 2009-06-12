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

import com.ibm.jaql.json.type.JsonEncoding;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonAtom;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

// TODO: appears to be unused

/**
 * 
 */
public abstract class JEncoder
{
  /**
   * @param item
   */
  public abstract void write(JsonValue value);

  /**
   * @param options
   */
  public abstract void setOptions(JsonRecord options);

  /**
   * @param atom
   */
  public abstract void writeAtom(JsonAtom atom);

  /**
   * @param array
   */
  public abstract void writeArrayBegin(JsonArray array);

  /**
   * 
   */
  public abstract void writeArrayEnd();

  /**
   * @param record
   */
  public abstract void writeRecordBegin(JsonRecord record);

  /**
   * 
   */
  public abstract void writeRecordEnd();

  /**
   * @param name
   */
  public abstract void writeFieldBegin(JsonString name);

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

  public abstract JsonRecord getOptions();

  public abstract DecodeType next();

  /**
   * Get the current atom. Only valid when next() just returned ATOM.
   * 
   * @return
   */
  public abstract JsonAtom getAtom();

  /**
   * Get the current field name. Only valid when next() just returned
   * FIELD_NAME.
   * 
   * @return
   */
  public abstract JsonString getName();

  public abstract void close();
}

abstract class JInput
{
  public abstract JsonRecord getOptions();
  public abstract JDecoder getDecoder();
  public abstract JsonEncoding getEncoding(int encodingId);
  public abstract JsonString getString(int stringId);
}

abstract class JOutput
{
  public abstract void setOptions(JsonRecord options);
  public abstract void setDecoder(JDecoder decoder);
  public abstract int getEncodingId(JsonEncoding encoding);
  public abstract long getStringId(JsonString string);
}

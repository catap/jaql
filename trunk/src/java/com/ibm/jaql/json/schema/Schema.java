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
package com.ibm.jaql.json.schema;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.json.type.Item;

/*******************************************************************************
 * At the moment, a scheme provides methods that concern schema matching and 
 * serialization of schema descriptions.
 * 
 * 
 * Data model: ----------- value ::= record | array | atom record ::= { (string:
 * value)* } array ::= [ (value)* ] atom ::= null | string | number | boolean |
 * ...
 * 
 * Schema language: ---------------- type ::= oneType ('|' oneType)* oneType ::=
 * anyType | atomType | arrayType | recordType anyType ::= '*' atomType ::= ID //
 * null | string | number | boolean | ... arrayType ::= [ ( type (',' type)*
 * (repeat)? )? ] typeList ::= repeat ::= <min,max> // min is integer >= 0, max
 * is integer >= min or * for unbounded | <count> // == <count,count> recordType
 * ::= { fieldType* } fieldType ::= name (fieldOpt)? ':' type fieldOpt ::= '*' //
 * zero or more fields with this prefix have this type | '?' // optional field
 * with this name and type
 * 
 * 
 ******************************************************************************/
public abstract class Schema
{
  protected final static byte UNKNOWN_TYPE = 0;
  protected final static byte ANY_TYPE     = 1;
  protected final static byte ATOM_TYPE    = 2;
  protected final static byte ARRAY_TYPE   = 3;
  protected final static byte RECORD_TYPE  = 4;
  protected final static byte OR_TYPE      = 5;

  public Schema               nextSchema;      // used by Array and Or Schemas

  /**
   * @param item
   * @return
   * @throws Exception
   */
  public abstract boolean matches(Item item) throws Exception;

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  public abstract String toString();

  /**
   * @param out
   * @throws IOException
   */
  public abstract void write(DataOutput out) throws IOException;

  /**
   * @param in
   * @return
   * @throws IOException
   */
  public static Schema read(DataInput in) throws IOException
  {
    byte b = in.readByte();
    switch (b)
    {
      case UNKNOWN_TYPE :
        return null;
      case ANY_TYPE :
        return new SchemaAny();
      case ATOM_TYPE :
        return new SchemaAtom(in);
      case ARRAY_TYPE :
        return new SchemaArray(in);
      case RECORD_TYPE :
        return new SchemaRecord(in);
      case OR_TYPE :
        return new SchemaOr(in);
      default :
        throw new IOException("invalid schema type: " + b);
    }
  }
}

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

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;

import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonValue;

// TODO: handling of null values

/*******************************************************************************
 * At the moment, a scheme provides methods that concern schema matching and 
 * serialization of schema descriptions.
 * 
 * 
 * Data model: 
 * ----------- 
 * value ::= record | array | atom 
 * record ::= { (string: value)* } 
 * array ::= [ (value)* ] 
 * atom ::= null | string | number | boolean | ...
 * 
 * Schema language: 
 * ---------------- 
 * type ::= oneType ('|' oneType)* 
 * oneType ::= anyType | atomType | arrayType | recordType 
 * anyType ::= '*' 
 * atomType ::= 'null' | string | number | boolean | ...
 * numberType ::= 'number' numberTypeArgs?
 * numberTypeArgs ::= '(' (
 *                          value             // constant
 *                        | min,max           // range
 *                        ) ')'
 *                           ')' )? 
 * arrayType ::= [ ( type (',' type)* (repeat)? )? ] 
 * typeList ::= 
 * repeat ::= <min,max> // min is integer >= 0, max is integer >= min or for unbounded 
 *          | <count>   // == <count,count> 
 * recordType ::= { fieldType*} 
 * fieldType ::= name (fieldOpt)? ':' type 
 * fieldOpt ::= '*' // zero or more fields with this prefix have this type 
 *            | '?' // optional field
 * with this name and type 
 ******************************************************************************/
public abstract class Schema
{
  public enum SchemaType
  {
    ANY, ARRAY, BINARY, DECIMAL, DOUBLE, GENERIC, LONG, NULL, NUMERIC, OR, RECORD, STRING
  }
  
  /**
   * @param item
   * @return
   * @throws Exception
   */
  public abstract boolean matches(JsonValue value) throws Exception;
  
  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    JsonSchema s = new JsonSchema(this);
    try
    {
      return JsonValue.printToString(s);
    } catch (IOException e)
    {
      throw new UndeclaredThrowableException(e);
    }
    
  }
  
  public abstract SchemaType getSchemaType();
}

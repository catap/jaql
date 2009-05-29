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

import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.Bool3;

/** Schema for a JSON record */
public class RecordSchema extends Schema
{
  protected Field[] fields; // never null
  protected Schema  rest; // Null if record is "closed"

  
  // -- inner classes -----------------------------------------------------------------------------
  
  /** Describes the field of a record */
  public static class Field
  {
    protected JsonString  name;
    protected Schema      schema;
    protected boolean     isOptional;

    public Field(JsonString name, Schema schema, boolean isOptional)
    {
      JaqlUtil.enforceNonNull(name);
      JaqlUtil.enforceNonNull(schema);
      
      this.name = new JsonString(name);
      this.isOptional = isOptional;
      this.schema = schema;
    }

    public JsonString getName()
    {
      return name;
    }
    
    public Schema getSchema()
    {
      return schema;
    }
    
    public boolean isOptional()
    {
      return isOptional;
    }
  }

  // accepts JsonString and Field
  public static final Comparator<Object> FIELD_BY_NAME_COMPARATOR = new Comparator<Object>() {
    @Override
    public int compare(Object o1, Object o2)
    {
      JsonString name1;
      if (o1 instanceof Field)
      {
        name1 = ((Field)o1).getName();
      }
      else
      {
        name1 = (JsonString)o1;
      }
      
      JsonString name2;
      if (o1 instanceof Field)
      {
        name2 = ((Field)o2).getName();
      }
      else
      {
        name2 = (JsonString)o2;
      }
      
      return name1.compareTo(name2);
    }      
  };
  

  // -- construction ------------------------------------------------------------------------------
  
  /**
   * 
   */
  public RecordSchema(Field[] fields, Schema rest)
  {
    // check
    if (fields == null) fields = new Field[0];
    checkFields(fields);
    
    // sort
    Arrays.sort(fields, FIELD_BY_NAME_COMPARATOR);
    
    // set
    this.fields = fields;
    this.rest = rest;
  }

  /** Checks whether all fields are valid. If not, throws an exception */
  private void checkFields(Field[] fields)
  {
    Set<JsonString> names = new TreeSet<JsonString>();
    for (Field f : fields)
    {
      if (names.contains(f.getName()))
      {
        throw new IllegalArgumentException("duplicate field names not allowed: " + f.getName());
      }
      names.add(f.getName());
    }
  }
  

  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.RECORD;
  }

  @Override
  public Bool3 isNull()
  {
    return Bool3.FALSE;
  }

  @Override
  public Bool3 isArray()
  {
    return Bool3.FALSE;
  }

  @Override
  public Bool3 isConst()
  {
    if (rest != null) {
      return Bool3.UNKNOWN;
    }
    Bool3 result = Bool3.TRUE;
    for (Field f : fields)
    {
      if (f.isOptional)
      {
        return Bool3.UNKNOWN;
      }
      result = result.and(f.schema.isConst());
    }
    return result;
  }

  @Override
  public boolean matches(JsonValue value) throws Exception
  {
    if (!(value instanceof JsonRecord))
    {
      return false;
    }
    JsonRecord rec = (JsonRecord) value;

    // assumption: field names are sorted
    int nr = rec.arity();
    int ns = fields.length;
    int pr = 0;
    int ps = 0;
    
    // zip join
    while (pr<nr && ps<ns)
    {
      Field schemaField = fields[ps];
      JsonString recordFieldName = rec.getName(pr);
      
      // compare
      int cmp = schemaField.getName().compareTo(recordFieldName);
      
      if (cmp < 0) 
      {
        // field is in schema but not in record
        if (!schemaField.isOptional)
        {
          return false;
        }
        ps++;
      }
      else if (cmp == 0)
      {
        // field is schema and in record
        if (!schemaField.getSchema().matches(rec.getValue(pr)))
        {
          return false;
        }
        ps++; pr++;
      }
      else
      {
        // field is not in schema but in record
        if (rest == null || !rest.matches(rec.getValue(pr)))
        {
          return false;
        }
        pr++;
      }
    }
    
    // only one of them still has fields, i.e., the while loops are exclusive
    while (pr < nr)
    {
      // there are fields left in the record
      if (rest == null || !rest.matches(rec.getValue(pr)))
      {
        return false;
      }
      pr++;
    }
    while (ps < ns)
    {
      // therea are fields left in the schema
      if (!fields[ps].isOptional)
      {
        return false;
      }
      ps++;
    }
    
    // everything ok
    return true;
  }

  
  // -- getters -----------------------------------------------------------------------------------

  public Field[] getFields()
  {
    return fields;
  }

  /**
   * @return The schema for unnamed fields.
   */
  public Schema getRest()
  {
    return rest;
  }

  public boolean isEmpty()
  {
    return fields.length==0 && rest==null;
  }
}

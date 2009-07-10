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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import com.ibm.jaql.json.type.JsonLong;
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
      if (o2 instanceof Field)
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
   * rest==null is closed
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
  
  public RecordSchema(List<Field> fields, Schema rest)
  {
    this(fields.toArray(new Field[fields.size()]), rest);
  }
  
  /** Matches any record */
  RecordSchema()
  {
    this((Field[])null, SchemaFactory.anyOrNullSchema());
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
  public Bool3 isArrayOrNull()
  {
    return Bool3.FALSE;
  }
  
  @Override
  public Bool3 isEmptyArrayOrNull()
  {
    return Bool3.FALSE;
  }

  @Override
  public boolean isConstant()
  {
    if (rest != null) {
      return false;
    }
    for (Field f : fields)
    {
      if (f.isOptional || !f.schema.isConstant())
      {
        return false;
      }
    }
    return true;
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
    int nr = rec.size();          // number of fields in record
    int ns = fields.length;       // number of fields in schema
    int pr = 0;                   // current field in record
    int ps = 0;                   // current field in schema
    
    // zip join
    Iterator<Entry<JsonString, JsonValue>> recIt = rec.iterator();
    Entry<JsonString, JsonValue> recEntry = null;
    if (nr > 0) recEntry = recIt.next();
    while (pr<nr && ps<ns)
    {
      Field schemaField = fields[ps];
      JsonString recordFieldName = recEntry.getKey();
      
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
        if (!schemaField.getSchema().matches(recEntry.getValue()))
        {
          return false;
        }
        ps++; pr++; 
        if (pr < nr)
        {
          assert recIt.hasNext();
          recEntry = recIt.next();
        }
      }
      else
      {
        // field is not in schema but in record
        if (rest == null || !rest.matches(recEntry.getValue()))
        {
          return false;
        }
        pr++; 
        if (pr < nr)
        {
          assert recIt.hasNext();
          recEntry = recIt.next();
        }
      }
    }
    
    // only one of them still has fields, i.e., the while loops are exclusive
    while (pr < nr)
    {
      // there are fields left in the record
      if (rest == null || !rest.matches(recEntry.getValue()))
      {
        return false;
      }
      pr++; 
      if (pr < nr)
      {
        assert recIt.hasNext();
        recEntry = recIt.next();
      }
    }
    assert !recIt.hasNext();
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

  public Field getField(JsonString name)
  {
    int index = Arrays.binarySearch(fields, name, FIELD_BY_NAME_COMPARATOR);
    if (index >= 0)
    {
      return fields[index];
    }
    return null;
  }
  
  @Override
  // -- merge -------------------------------------------------------------------------------------

  protected Schema merge(Schema other)
  {
    if (other instanceof RecordSchema)
    {
      RecordSchema o = (RecordSchema)other;
      
      Field[] myFields = this.fields;
      Field[] otherFields = o.fields;
     
      // assumption: field names are sorted
      int myN = myFields.length;
      int otherN = otherFields.length;
      int myPos = 0;
      int otherPos = 0;
      
      // zip join
      List<Field> newFields = new LinkedList<Field>();
      while (myPos<myN && otherPos<otherN)
      {
        Field myField = myFields[myPos];
        Field otherField = otherFields[otherPos];
        
        // compare
        int cmp = myField.getName().compareTo(otherField.getName());
        
        if (cmp < 0) 
        {
          // I have the field, but other has not --> make it optional
          newFields.add(new Field(myField.name, myField.schema, true));
          myPos++;
        }
        else if (cmp == 0)
        {
          // both have field --> keep
          newFields.add(new Field(myField.name, SchemaTransformation.merge(myField.schema, otherField.schema), 
              myField.isOptional || otherField.isOptional));
          myPos++; otherPos++;
        }
        else
        {
          // I don't have the field, but other has --> make it optional
          newFields.add(new Field(otherField.name, otherField.schema, true));
          otherPos++;
        }
      }
      
      // only one of them still has fields, i.e., the while loops are exclusive
      while (myPos < myN)
      {
        Field myField = myFields[myPos];
        newFields.add(new Field(myField.name, myField.schema, true));
        myPos++;
      }
      while (otherPos < otherN)
      {
        Field otherField = otherFields[otherPos];
        newFields.add(new Field(otherField.name, otherField.schema, true));
        otherPos++;
      }
      
      // deal with rest
      Schema newRest = null;
      if (this.rest != null)
      {
        if (o.rest != null) 
        {
          newRest = SchemaTransformation.merge(this.rest, o.rest);
        }
        else
        {
          newRest = rest;
        }
      }
      else if (o.rest != null)
      {
        newRest = o.rest;
      }

      // done
      return new RecordSchema(newFields.toArray(new Field[newFields.size()]), newRest);
    }
    return null;
  }
  
  @Override
  public Schema elements()
  {
    Schema result = null;
    for (int i=0; i<fields.length; i++)
    {
      Schema s = fields[i].getSchema();
      if (result == null)
      {
        result = s;
      }
      else
      {
        result = SchemaTransformation.merge(result, s);
      }
    }
    if (rest != null)
    {
      if (result == null)
      {
        result = rest;
      }
      else
      {
        result = SchemaTransformation.merge(result, rest);
      }
    }
    return result; 
  }
  
  // -- introspection -----------------------------------------------------------------------------

  @Override
  public Bool3 hasElement(JsonValue which)
  {
    if (which instanceof JsonString)
    {
      Field field = getField((JsonString)which);
      if (field==null)
      {
        return rest==null ? Bool3.FALSE : Bool3.UNKNOWN;
      }
      else
      {
        return field.isOptional ? Bool3.UNKNOWN : Bool3.TRUE;
      }
    }
    return Bool3.FALSE;
  }
  
  @Override
  public Schema element(JsonValue which)
  {
    if (which instanceof JsonString)
    {
      Field field = getField((JsonString)which);
      if (field == null)
      {
        return rest;
      }
      else
      {
        return field.getSchema(); // TODO: add nullability when optional?
      }
    }
    return null; 
  }
  
  @Override
  public JsonLong minElements()
  {
    long l = 0;
    for (Field f : fields)
    {
      if (!f.isOptional) 
      {
        l++;
      }
    }
    return new JsonLong(l);
  }

  @Override
  public JsonLong maxElements()
  {
    if (rest != null) 
    {
      return null;
    }
    else
    {
      return new JsonLong(fields.length);
    }    
  } 
}

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

import java.util.List;

import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.Bool3;

/** Schema for an array. Has individual schemata for the first k elements and a single schema 
 * for the remaining elements, if any. */
public final class ArraySchema extends Schema
{
  private Schema[]          head;                 // list of schemas for the first elements (never null)
  private Schema            rest;                 // schema of the remaining elements (can be null)
  
  // -- construction ------------------------------------------------------------------------------
  
  /** head and rest */
  public ArraySchema(Schema[] head, Schema rest)
  {
    if (head==null) head = new Schema[0];
    
    // init
    this.head = new Schema[head.length];
    System.arraycopy(head, 0, this.head, 0, head.length);
    this.rest = rest;
  }

  /** no rest */
  public ArraySchema(Schema[] head)
  {
    this(head, null);
  }
  
  /** No rest */
  public ArraySchema(List<Schema> schemata)
  {
    this(schemata.toArray(new Schema[schemata.size()]), null);
  }
  
  /** Matches any array */
  ArraySchema()
  {
    this(new Schema[0], SchemaFactory.anySchema()); 
  }
  
  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.ARRAY;
  }
  
  @Override
  public boolean hasModifiers()
  {
    return false;
  }
  
  @Override
  public boolean isConstant()
  {
    for (Schema s : head)
    {
      if (!s.isConstant()) return false;
    }
    return rest == null;    
  }

  public JsonArray getConstant()
  {
    if (!isConstant())
      return null;
    BufferedJsonArray a = new BufferedJsonArray();
    for (Schema s : head)
    {
      a.add(s.getConstant());
    }
    return a;
  }
  
  public Bool3 isEmpty()
  {
    if ( head.length==0 )
    {
      return rest==null ? Bool3.TRUE : Bool3.UNKNOWN ;
    }
    else
    {
      return Bool3.FALSE;
    } 
  }
  
  @Override
  public Bool3 isEmpty(JsonType type, JsonType ... types)
  {
    return is(type, types).and(isEmpty());
  }
  
  @SuppressWarnings("unchecked")
  @Override 
  public Class<? extends JsonValue>[] matchedClasses()
  {
    return new Class[] { JsonArray.class }; 
  }

  @Override
  public boolean matches(JsonValue value) throws Exception
  {
    if (!(value instanceof JsonArray))
    {
      return false;
    }
    
    // check array head
    JsonArray arr = (JsonArray) value;
    JsonIterator iter = arr.iter();
    for (Schema s : head)
    {
      if (!iter.moveNext() || !s.matches(iter.current()))
      {
        return false;
      }
    }
    
    // when no rest, check that array is fully read
    if (rest==null) 
    {
      return !iter.moveNext();
    }

    // there is a rest; check it
    while (iter.moveNext())
    {
      if (!rest.matches(iter.current()))
      {
        return false;
      }  
    }
    return true;
  }
  
  // -- getters -----------------------------------------------------------------------------------
  
  public List<Schema> getHeadSchemata()
  {
    return JaqlUtil.toUnmodifiableList(head);
  }
  
  public Schema getRestSchema()
  {
    return rest;
  }
  
  public boolean hasRest() 
  {
    return rest != null;    
  }
  

  // -- merge -------------------------------------------------------------------------------------

  @Override
  protected Schema merge(Schema other)
  {
    if (other instanceof ArraySchema)
    {
      ArraySchema o = (ArraySchema)other;
      
      // keep empty arrays separate
      if ((o.isEmpty().always() && this.isEmpty().never()) ||
          (this.isEmpty().always() && o.isEmpty().never()))
      {
        return null; 
      }
      
      // determine number of first elements with explicit schema
      int minLength = Math.min(this.head.length, o.head.length);
      int newHeadLength = Math.max(this.head.length, o.head.length);
      if (this.rest == null || o.rest == null) newHeadLength = minLength;
      
      // derive schemata of those first elements
      Schema[] newHead = new Schema[newHeadLength];
      for (int i=0; i<newHeadLength; i++)
      {
        Schema fromMe = i<this.head.length ? this.head[i] : this.rest;
        Schema fromOther = i<o.head.length ? o.head[i] : o.rest;
        newHead[i] = SchemaTransformation.merge(fromMe, fromOther);
      }
      
      // determine schema of rest
      Schema newRest = null;
      for (int i=newHeadLength; i<this.head.length; i++)
      {
        newRest = newRest == null ? this.head[i]
                                  : SchemaTransformation.merge(newRest, this.head[i]);
      }
      for (int i=newHeadLength; i<o.head.length; i++)
      {
        newRest = newRest == null ? o.head[i]
                                  : SchemaTransformation.merge(newRest, o.head[i]);
      }
      if (this.rest != null)
      {
        newRest = newRest == null ? this.rest
                                  : SchemaTransformation.merge(newRest, this.rest);
      }
      if (o.rest != null)
      {
        newRest = newRest == null ? o.rest
                                  : SchemaTransformation.merge(newRest, o.rest);
      }
      
      // done
      return new ArraySchema(newHead, newRest);
    }
    return null;
  }

  
  // -- introspection -----------------------------------------------------------------------------

  @Override
  public Schema elements()
  {
    if (head.length == 0)
    {
      return rest;
    }
    
    Schema combinedSchema = head[0];
    for (int i=1; i<head.length; i++)
    {
      combinedSchema = SchemaTransformation.merge(combinedSchema, head[i]);
    }
    if (rest != null)
    {
      combinedSchema = SchemaTransformation.merge(combinedSchema, rest);
    }
    return combinedSchema;
  }
  
  @Override
  public Bool3 hasElement(JsonValue which)
  {
    if (which instanceof JsonLong)
    {
      long index = ((JsonLong)which).get();
      if (index < head.length)
      {
        return Bool3.TRUE;
      }
      else if (rest != null)
      {
        return Bool3.UNKNOWN;
      }
      else
      {
        return Bool3.FALSE;
      }
    }
    return Bool3.FALSE;
  }
  
  @Override
  public Schema element(JsonValue which)
  {
    if (which instanceof JsonNumber)
    {
      long index = ((JsonNumber)which).longValueExact();
      if( index < 0 )
      {
        return SchemaFactory.nullSchema();
      }
      else if (index < head.length)
      {
        return head[(int)index];
      }
      else  
      {
        return rest; // can be null
      }
    }
    return null; 
  }

  @Override
  public JsonLong minElements()
  {
    return JsonLong.make(head.length);
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
      return JsonLong.make(head.length);
    }
  }
  
  // -- comparison --------------------------------------------------------------------------------
  
  @Override
  public int compareTo(Schema other)
  {
    int c = this.getSchemaType().compareTo(other.getSchemaType());
    if (c != 0) return c;
    
    ArraySchema o = (ArraySchema)other;
    c = SchemaUtil.arrayCompare(this.head, o.head);
    if (c != 0) return c;
    c = SchemaUtil.compare(this.rest, o.rest);
    return c;
  } 
}

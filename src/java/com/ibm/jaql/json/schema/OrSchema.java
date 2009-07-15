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

import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.Bool3;

/** Schema that matches if at least one of the provided schemata matches. */
public class OrSchema extends Schema
{
  protected Schema[] schemata;    // list of alternatives, never null, does not contain OrSchema

  // -- construction ------------------------------------------------------------------------------
  
  OrSchema(Schema[] schemata)
  {
    if (schemata.length == 0)
    {
      throw new IllegalArgumentException("at least one schema has to be provided");
    }
    for (int i=0; i<schemata.length; i++)
    {
      if (schemata[i] instanceof OrSchema)
      {
        throw new IllegalArgumentException("Schema alternatives cannot be nested");
      }
    }
    this.schemata = schemata;
  }

  OrSchema(List<Schema> schemata)
  {
    this(schemata.toArray(new Schema[schemata.size()]));
  }
  
  OrSchema(Schema s1, Schema s2, Schema s3)
  {
    this(new Schema[] { s1, s2, s3 });
  }

  OrSchema(Schema s1, Schema s2)
  {
    this(new Schema[] { s1, s2 });
  }

  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.OR;
  }
  
  @Override
  public Bool3 isNull()
  {
    Bool3 result = schemata[0].isNull();
    switch (result)
    {
    case TRUE:
      // check whether all are true
      for (int i=1; i<schemata.length; i++) 
      {
        if (!schemata[i].isNull().always()) 
        {
          return Bool3.UNKNOWN;
        }
      }
      return Bool3.TRUE;
    
    case FALSE:
      // check whether all are false
      for (int i=1; i<schemata.length; i++) 
      {
        if (!schemata[i].isNull().never()) 
        {
          return Bool3.UNKNOWN;
        }
      }
      return Bool3.FALSE;      
    
    case UNKNOWN:
      /// otherwise we don't know
      return Bool3.UNKNOWN;
    
    default:
      throw new IllegalStateException();
    }
  }
  
  @Override
  public Bool3 isArrayOrNull()
  {
    Bool3 result = schemata[0].isArrayOrNull();
    switch (result)
    {
    case TRUE:
      // check whether all are true
      for (int i=1; i<schemata.length; i++) 
      {
        if (!schemata[i].isArrayOrNull().always()) 
        {
          return Bool3.UNKNOWN;
        }
      }
      return Bool3.TRUE;
    
    case FALSE:
      // check whether all are false
      for (int i=1; i<schemata.length; i++) 
      {
        if (!schemata[i].isArrayOrNull().never()) 
        {
          return Bool3.UNKNOWN;
        }
      }
      return Bool3.FALSE;      
    
    case UNKNOWN:
      /// otherwise we don't know
      return Bool3.UNKNOWN;
    
    default:
      throw new IllegalStateException();
    }
  }
  
  @Override
  public Bool3 isEmptyArrayOrNull()
  {
    Bool3 result = schemata[0].isEmptyArrayOrNull();
    switch (result)
    {
    case TRUE:
      // check whether all are true
      for (int i=1; i<schemata.length; i++) 
      {
        if (!schemata[i].isEmptyArrayOrNull().always()) 
        {
          return Bool3.UNKNOWN;
        }
      }
      return Bool3.TRUE;
    
    case FALSE:
      // check whether all are false
      for (int i=1; i<schemata.length; i++) 
      {
        if (!schemata[i].isEmptyArrayOrNull().never()) 
        {
          return Bool3.UNKNOWN;
        }
      }
      return Bool3.FALSE;      
    
    case UNKNOWN:
      /// otherwise we don't know
      return Bool3.UNKNOWN;
    
    default:
      throw new IllegalStateException();
    }
  }

  @Override
  public boolean isConstant()
  {
    return schemata.length == 1 && schemata[0].isConstant(); 
  }

  @Override
  public boolean matches(JsonValue value) throws Exception
  {
    for (Schema s : schemata)
    {
      if (s.matches(value))
      {
        return true;
      }
    }
    return false;
  }
  
  
  // -- getters -----------------------------------------------------------------------------------
  
  public Schema[] getInternal()
  {
    return schemata;
  }
  

  // -- merge -------------------------------------------------------------------------------------

  @Override
  protected Schema merge(Schema other)
  {
    if (other instanceof OrSchema)
    {
      // inefficient; O(n^2)
      Schema result = this;
      for (Schema schema : ((OrSchema)other).schemata)
      {
        result = result.merge(schema); // result is orschema: never returns null and will not nest
      }
      return result;
    }
    else
    {
      // try to merge it into: O(n)
      for(int i=0; i<schemata.length; i++)
      {
        Schema mergedSchema = schemata[i].merge(other);
        if (mergedSchema != null)
        {
          Schema[] newSchemata;
          if( mergedSchema instanceof OrSchema )
          {
            // Flatten nested OrSchemas
            OrSchema orSchema = (OrSchema)mergedSchema;
            newSchemata = new Schema[schemata.length + orSchema.schemata.length - 1];
            System.arraycopy(schemata, 0, newSchemata, 0, i);
            System.arraycopy(schemata, i+1, newSchemata, i, schemata.length - i - 1);
            System.arraycopy(orSchema.schemata, 0, newSchemata, schemata.length - 1, orSchema.schemata.length);
          }
          else
          {
            newSchemata = new Schema[schemata.length];
            System.arraycopy(schemata, 0, newSchemata, 0, schemata.length);
            newSchemata[i] = mergedSchema;
          }
          return new OrSchema(newSchemata);
        }
      }
      
      // not possible, add it as new alternative
      Schema[] newSchemata = new Schema[schemata.length+1];
      System.arraycopy(schemata, 0, newSchemata, 0, schemata.length);
      newSchemata[schemata.length] = other;
      return new OrSchema(newSchemata);
    }
  }

  // -- introspection -----------------------------------------------------------------------------

  @Override
  public Bool3 hasElement(JsonValue which)
  {
    Bool3 result = schemata[0].hasElement(which);
    switch (result)
    {
    case TRUE:
      // check whether all are true
      for (int i=1; i<schemata.length; i++) 
      {
        if (schemata[i].hasElement(which).maybeNot()) 
        {
          return Bool3.UNKNOWN;
        }
      }
      return Bool3.TRUE;
    
    case FALSE:
      // check whether all are false
      for (int i=1; i<schemata.length; i++) 
      {
        if (schemata[i].isNull().maybe()) 
        {
          return Bool3.UNKNOWN;
        }
      }
      return Bool3.FALSE;      
    
    case UNKNOWN:
      /// otherwise we don't know
      return Bool3.UNKNOWN;
    
    default:
      throw new IllegalStateException();
    }
  }
  
  @Override
  public Schema elements()
  {
    Schema result = null;
    for (int i=0; i<schemata.length; i++)
    {
      Schema s = schemata[i].elements();
      if (s!=null)
      {
        result = result == null ? s : SchemaTransformation.merge(result, s);
      }
    }
    return result; 
  }
  
  @Override
  public JsonLong minElements()
  {
    JsonLong result = schemata[0].minElements();
    for (int i=1; i<schemata.length && result != null; i++)
    {
      JsonLong l = schemata[i].minElements();
      if (l == null)
      {
        result = null;
      }
      else
      {
        result.set(Math.min(result.get(), l.get()));
      }
      
    }
    return result;
  }

  @Override
  public JsonLong maxElements()
  {
    JsonLong result = schemata[0].maxElements();
    for (int i=1; i<schemata.length && result != null; i++)
    {
      JsonLong l = schemata[i].maxElements();
      if (l == null)
      {
        result = null;
      }
      else
      {
        result.set(Math.max(result.get(), l.get()));
      }
      
    }
    return result;
  }
}

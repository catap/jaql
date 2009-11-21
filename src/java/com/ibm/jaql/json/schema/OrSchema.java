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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.Bool3;

/** Schema that matches if at least one of the provided schemata matches. This class cannot be
 * instantiated directly; instances may be obtained using {@link #make(Schema...)}. */
public final class OrSchema extends Schema
{
  protected Schema[] schemata;    // list of alternatives, never null, does not contain OrSchema, kept sorted

  // -- construction ------------------------------------------------------------------------------
  
  private OrSchema(Schema[] schemata)
  {
    this.schemata = new Schema[schemata.length];
    System.arraycopy(schemata, 0, this.schemata, 0, schemata.length);
  }
  
  /** Combines its argument schemata. The resulting schema will match a value if and only if it
   * is matched by one of the provided schemata. The method does not necessarily return an 
   * instance or <code>OrSchema</code>. */
  public static Schema make(Schema ... schemata)
  {
    if (schemata.length == 0)
    {
      throw new IllegalArgumentException("at least one schema has to be provided");
    }

    // unnest, sort and remove duplicates (TODO: could be more efficient)
    SortedSet<Schema> sortedSet = new TreeSet<Schema>();
    for (Schema s : schemata)
    {
      if (s instanceof OrSchema)
      {
        sortedSet.addAll( Arrays.asList( ((OrSchema)s).schemata ) );
      }
      else
      {
        sortedSet.add(s);
      }
    }
    
    // return result
    if (sortedSet.size() == 1)
    {
      return sortedSet.first();
    }
    else
    {
      return new OrSchema(sortedSet.toArray(new Schema[sortedSet.size()]));
    }
  }

  /** Combines its argument schemata. The resulting schema will match a value if and only if it
   * is matched by one of the provided schemata. The method does not necessarily return an 
   * instance or <code>OrSchema</code>. */
  public static Schema make(List<Schema> schemata)
  {
    return make(schemata.toArray(new Schema[schemata.size()]));
  }
  
 
  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.OR;
  }

  
  @Override
  public boolean hasModifiers()
  {
    for (Schema s : schemata)
    {
      if (s.hasModifiers()) return true;
    }
    return false;
  }
  
  @SuppressWarnings("unchecked")
  @Override 
  public Class<? extends JsonValue>[] matchedClasses()
  {
    Set<Class<? extends JsonValue>> classes = new HashSet<Class<? extends JsonValue>>();
    for (Schema s : schemata) {
      for (Class<? extends JsonValue> clazz : s.matchedClasses()) 
      {
        classes.add(clazz);
      }
    }
    return classes.toArray(new Class[classes.size()]); 
  }
  
  @Override
  public Bool3 is(JsonType type, JsonType ... types)
  {
    Bool3 result = schemata[0].is(type, types);
    switch (result)
    {
    case TRUE:
      // check whether all are true
      for (int i=1; i<schemata.length; i++) 
      {
        if (!schemata[i].is(type, types).always()) 
        {
          return Bool3.UNKNOWN;
        }
      }
      return Bool3.TRUE;
    
    case FALSE:
      // check whether all are false
      for (int i=1; i<schemata.length; i++) 
      {
        if (!schemata[i].is(type, types).never()) 
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
  public Bool3 isEmpty(JsonType type, JsonType ... types)
  {
    Bool3 result = schemata[0].isEmpty(type, types);
    switch (result)
    {
    case TRUE:
      // check whether all are true
      for (int i=1; i<schemata.length; i++) 
      {
        if (!schemata[i].isEmpty(type, types).always()) 
        {
          return Bool3.UNKNOWN;
        }
      }
      return Bool3.TRUE;
    
    case FALSE:
      // check whether all are false
      for (int i=1; i<schemata.length; i++) 
      {
        if (!schemata[i].isEmpty(type, types).never()) 
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
  
  public JsonValue getConstant()
  {
    if (isConstant())
      return schemata[0].getConstant();
    else
      return null;
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
  
  public List<Schema> get()
  {
    return JaqlUtil.toUnmodifiableList(schemata);
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
          newSchemata = new Schema[schemata.length];
          System.arraycopy(schemata, 0, newSchemata, 0, schemata.length);
          newSchemata[i] = mergedSchema;
          return OrSchema.make(newSchemata);
        }
      }
      
      // not possible, add it as new alternative
      Schema[] newSchemata = new Schema[schemata.length+1];
      System.arraycopy(schemata, 0, newSchemata, 0, schemata.length);
      newSchemata[schemata.length] = other;
      return OrSchema.make(newSchemata);
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
        if (schemata[i].hasElement(which).maybe()) 
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
  public Schema element(JsonValue which)
  {
    Schema result = null;
    for (int i=0; i<schemata.length; i++)
    {
      Schema s = schemata[i].element(which);
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
    JsonLong m = schemata[0].minElements();
    MutableJsonLong result = m == null ? null : new MutableJsonLong(m);
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
    JsonLong m = schemata[0].maxElements();
    MutableJsonLong result = m == null ? null : new MutableJsonLong(m);
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
  
  // -- comparison --------------------------------------------------------------------------------

  @Override
  public int compareTo(Schema other)
  {
    int c = this.getSchemaType().compareTo(other.getSchemaType());
    if (c != 0) return c;
    
    OrSchema o = (OrSchema)other;
    return SchemaUtil.arrayCompare(this.schemata, o.schemata);
  }
}

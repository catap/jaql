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
package com.ibm.jaql.json.type;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.util.BaseUtil;

/** A JSON record, i.e., a set of fields = (key, value)-pairs. Compatibility with the Java
 * Collection Framework is provided via the {@link #asMap()} method. */
public abstract class JsonRecord extends JsonValue 
implements Iterable<Entry<JsonString, JsonValue>>
{
  public static final JsonRecord EMPTY = new BufferedJsonRecord(); // TODO: should be immutable
  private static final JsonValue UNIQUE_VALUE = new JsonLong(0); // doesn't matter which, but must have unqiue identity
  
  
  // -- abstract methods --------------------------------------------------------------------------

  /** Returns the number of fields present in this record. */
  public abstract int size();
  
  /** Checks whether this record contains the specified key (i.e., field name) */
  public abstract boolean containsKey(JsonString key);
  
  /** Returns the value for the specified key (i.e., field name) or <code>defaultValue</code> 
   * if the key doesn't exist. */
  public abstract JsonValue get(JsonString key, JsonValue defaultValue);

  /** Returns an iterator over the fields in this record in arbitrary order. Entries returned by 
   * the iterator are valid only until the next call to the {@link Iterator#next()} method. */
  @Override
  public abstract Iterator<Entry<JsonString, JsonValue>> iterator();
  
  /** Returns an iterator over the fields in this record in field-name order. Entries returned by 
   * the iterator are valid only until the next call to the {@link Iterator#next()} method. */
  public abstract Iterator<Entry<JsonString, JsonValue>> iteratorSorted();

  // overrides return type of {@link JsonValue#getCopy(JsonValue)}
  @Override
  public abstract JsonRecord getCopy(JsonValue target) throws Exception;
  
  
  // -- default implementations -------------------------------------------------------------------
  
  /** Checks whether this record is empty */
  public boolean isEmpty()
  {
    return size()==0;
  }

  /** Returns the value for the specified key (i.e., field name) or <code>null</code> if the
   * key doesn't exist. */
  public JsonValue get(JsonString key)
  {
    return get(key, null);
  }

  /** Returns the value for the specified key (i.e., field name) or throws an 
   * {@link IllegalArgumentException} if the key doesn't exist. */
  public JsonValue getRequired(JsonString key)
  {
    JsonValue value = get(key, UNIQUE_VALUE);
    if (value == UNIQUE_VALUE) // identity check!
    {
      throw new IllegalArgumentException("invalid field name " + key);
    }
    return value;
  }
  
  /** Checks whether this record contains the specified value. */
  public boolean containsValue(JsonValue value)
  {
    Iterator<Entry<JsonString, JsonValue>> it = iterator();
    while (it.hasNext())
    {
      Entry<JsonString, JsonValue> entry = it.next();
      if (JsonUtil.equals(entry.getValue(), value)) 
      {
        return true;
      }
    }
    return false;
  }

  
  // -- hashing/comparison ------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonValue#compareTo(java.lang.Object) */
  public final int compareTo(Object arg0)
  {
    JsonRecord t = (JsonRecord) arg0;
    int cmp;
    Iterator<Entry<JsonString, JsonValue>> i1 = this.iteratorSorted();
    Iterator<Entry<JsonString, JsonValue>> i2 = t.iteratorSorted();
    int s1 = size();
    int s2 = t.size();
    int size = Math.min(s1, s2);
    for (int i = 0; i < size; i++)
    {
      assert i1.hasNext(); 
      Entry<JsonString, JsonValue> e1 = i1.next();
      assert i2.hasNext(); 
      Entry<JsonString, JsonValue> e2 = i2.next();
      
      cmp = JsonUtil.compare(e1.getKey(), e2.getKey());
      if (cmp != 0)
      {
        return cmp;
      }

      cmp = JsonUtil.compare(e1.getValue(), e2.getValue());
      if (cmp != 0)
      {
        return cmp;
      }
    }
    return s1-s2;
  }

  /* @see com.ibm.jaql.json.type.JsonValue#longHashCode() */
  @Override
  public long longHashCode()
  {
    long h = BaseUtil.GOLDEN_RATIO_64;
    Iterator<Entry<JsonString, JsonValue>> it = iteratorSorted();
    while (it.hasNext())
    {
      Entry<JsonString, JsonValue> entry = it.next();
      h ^= JsonUtil.longHashCode(entry.getKey());
      h *= BaseUtil.GOLDEN_RATIO_64;
      h ^= JsonUtil.longHashCode(entry.getValue());
      h *= BaseUtil.GOLDEN_RATIO_64;
    }
    return h >> 32;
  }
  

  // -- static methods for iterator conversion ----------------------------------------------------
  
  /** Produces a JsonIterator for the keys in the provided iterator. */
  public static JsonIterator keyIter(final Iterator<Entry<JsonString, JsonValue>> iterator)
  {
    return new JsonIterator() {
      public boolean moveNext() throws Exception
      {
        if (!iterator.hasNext())
        {
          return false;
        }
        Entry<JsonString, JsonValue> e = iterator.next();
        currentValue = e.getKey();
        return true; 
      }
    };
  }

  /** Produces a JsonIterator for the values in the provided iterator. */
  public static JsonIterator valueIter(final Iterator<Entry<JsonString, JsonValue>> iterator)
  {
    return new JsonIterator() {
      public boolean moveNext() throws Exception
      {
        if (!iterator.hasNext())
        {
          return false;
        }
        Entry<JsonString, JsonValue> e = iterator.next();
        currentValue = e.getValue();
        return true; 
      }
    };
  }

  /** Produces a JsonIterator with [key, value] entries in the provided iterator. */
  public static JsonIterator keyValueIter(final Iterator<Entry<JsonString, JsonValue>> iterator)
  {
    final BufferedJsonArray pair = new BufferedJsonArray(2);
    return new JsonIterator(pair) {
      public boolean moveNext() throws Exception
      {
        if (!iterator.hasNext())
        {
          return false;
        }
        Entry<JsonString, JsonValue> e = iterator.next();
        pair.set(0, e.getKey());
        pair.set(1, e.getValue());
        return true; // currentValue == pair
      }
    };
  }


  // -- Map compatibility -------------------------------------------------------------------------

  /** Returns an unmodifiable map view of this JSON record */
  public Map<JsonString, JsonValue> asMap()
  {
    return new JsonRecordAsMap();
  }
  
  /** A map view of this JSON record */
  private class JsonRecordAsMap extends AbstractMap<JsonString, JsonValue>
  {
    @Override
    public int size()
    {
      return JsonRecord.this.size();
    }
    
    @Override
    public boolean containsKey(Object key)
    {
      if (key instanceof JsonString)
      {
        return JsonRecord.this.containsKey((JsonString)key);
      }
      return false;
    }
    
    @Override
    public boolean containsValue(Object value)
    {
      if (value instanceof JsonValue)
      {
        return JsonRecord.this.containsValue((JsonValue)value);
      }
      return false;
    }
    
    @Override
    public JsonValue get(Object key)
    {
      if (key instanceof JsonString)
      {
        return JsonRecord.this.get((JsonString)key);
      }
      return null;
    }
    
    @Override
    public Set<Entry<JsonString, JsonValue>> entrySet()
    {
      return new AbstractSet<Entry<JsonString, JsonValue>>()
      {
        @Override
        public Iterator<Entry<JsonString, JsonValue>> iterator()
        {
          return JsonRecord.this.iterator();
        }

        @Override
        public int size()
        {
          return JsonRecord.this.size();
        }
      };
    };
  }
  
  
  // -- Entry class (can be used by subclasses) ----------------------------------------------

  /** A class that holds record entries. Can be used by subclasses of JsonRecord to implement
   * the {@link #iterator()} method. */
  protected class RecordEntry implements Entry<JsonString, JsonValue>
  {
    protected JsonString name; // can be set by subclasses of JsonRecord
    protected JsonValue value; // can be set by subclasses of JsonRecord
    
    RecordEntry(JsonString name, JsonValue value)
    {
      this.name = name;
      this.value = value;
    }
    
    RecordEntry()
    {
    }

    @Override
    public JsonString getKey()
    {
      return name;
    }

    @Override
    public JsonValue getValue()
    {
      return value;
    }

    /** Unsupported operation */
    @Override
    public final JsonValue setValue(JsonValue value)
    {
      throw new UnsupportedOperationException();
    }    
  }
}

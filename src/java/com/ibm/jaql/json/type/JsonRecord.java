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

import java.util.Iterator;
import java.util.Map.Entry;

import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.util.BaseUtil;

/** A JSON record, i.e., a set of fields = (key, value)-pairs. */
public abstract class JsonRecord extends JsonValue 
implements Iterable<Entry<JsonString, JsonValue>>
{
  public static final JsonRecord EMPTY = new BufferedJsonRecord(); // TODO: should be immutable

  
  // -- abstract methods --------------------------------------------------------------------------

  /** Returns the number of fields present in this record. */
  public abstract int size();
  
  /** Checks whether this record contains the specified key (i.e., field name) */
  public abstract boolean containsKey(JsonString key);
  
  /** Returns the value for the specified key (i.e., field name) or <code>defaultValue</code> 
   * if the key doesn't exist. */
  public abstract JsonValue get(JsonString key, JsonValue defaultValue);

  /** Returns the value for the specified key (i.e., field name) or throws an 
   * {@link IllegalArgumentException} if the key doesn't exist. */
  public abstract JsonValue getRequired(JsonString key);

  /** Returns an iterator over the fields in this record. Entries returned by the iterator are
   * valid only until the next call to the {@link Iterator#next()} method. The fields are produced
   * in ascending order of their field name. */
  public abstract Iterator<Entry<JsonString, JsonValue>> iterator();

  /* Overrides return type of {@link JsonValue#getCopy(JsonValue)}. */
  public abstract JsonRecord getCopy(JsonValue target) throws Exception;
  
  // -- default implementations -------------------------------------------------------------------
  
  /** Returns the value for the specified key (i.e., field name) or <code>null</code> if the
   * key doesn't exist. */
  public JsonValue get(JsonString key)
  {
    return get(key, null);
  }

  /** Returns an iterator over the field names in this record (in ascending order). */
  public JsonIterator keyIter()
  {
    final Iterator<Entry<JsonString, JsonValue>> entries = iterator();
    return new JsonIterator() {
      public boolean moveNext() throws Exception
      {
        if (!entries.hasNext())
        {
          return false;
        }
        Entry<JsonString, JsonValue> e = entries.next();
        currentValue = e.getKey();
        return true; 
      }
    };
  }
  
  /** Returns an iterator over the field values in this record (in ascending order of 
   * field names). */
  public JsonIterator valueIter()
  {
    final Iterator<Entry<JsonString, JsonValue>> entries = iterator();
    return new JsonIterator() {
      public boolean moveNext() throws Exception
      {
        if (!entries.hasNext())
        {
          return false;
        }
        Entry<JsonString, JsonValue> e = entries.next();
        currentValue = e.getValue();
        return true; 
      }
    };
  }
  
  /** Returns an iterator over the fields in this record (in ascending order of field names),
   * represented as arrays of form [key, value].*/
  public JsonIterator keyValueIter()
  {
    final Iterator<Entry<JsonString, JsonValue>> entries = iterator();
    final BufferedJsonArray pair = new BufferedJsonArray(2);
    return new JsonIterator(pair) {
      public boolean moveNext() throws Exception
      {
        if (!entries.hasNext())
        {
          return false;
        }
        Entry<JsonString, JsonValue> e = entries.next();
        pair.set(0, e.getKey());
        pair.set(1, e.getValue());
        return true; // currentValue == pair
      }
    };
  }
  
  // -- hashing/comparison ------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonValue#compareTo(java.lang.Object) */
  public final int compareTo(Object arg0)
  {
    // TODO: this should be order-independent
    JsonRecord t = (JsonRecord) arg0;
    int cmp;
    Iterator<Entry<JsonString, JsonValue>> i1 = this.iterator();
    Iterator<Entry<JsonString, JsonValue>> i2 = t.iterator();
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
    if (s1 == s2)
    {
      return 0;
    }
    else if (s1 < s2)
    {
      return -1;
    }
    else
    {
      assert s1>s2;
      return 1;
    }
  }

  /* @see com.ibm.jaql.json.type.JsonValue#hashCode() */
  @Override
  public int hashCode()
  {
    int h = BaseUtil.GOLDEN_RATIO_32;
    for (Entry<JsonString, JsonValue> entry : this)
    {
      h ^= JsonUtil.hashCode(entry.getKey());
      h *= BaseUtil.GOLDEN_RATIO_32;
      h ^= JsonUtil.hashCode(entry.getValue());
      h *= BaseUtil.GOLDEN_RATIO_32;
    }
    return (int) (h >> 32);
  }
  
  /* @see com.ibm.jaql.json.type.JsonValue#longHashCode() */
  @Override
  public long longHashCode()
  {
    long h = BaseUtil.GOLDEN_RATIO_64;
    for (Entry<JsonString, JsonValue> entry : this)
    {
      h ^= JsonUtil.longHashCode(entry.getKey());
      h *= BaseUtil.GOLDEN_RATIO_64;
      h ^= JsonUtil.longHashCode(entry.getValue());
      h *= BaseUtil.GOLDEN_RATIO_64;
    }
    return (int) (h >> 32);
  }
  

  // -- Iterable interface ------------------------------------------------------------------------

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

    @Override
    public JsonValue setValue(JsonValue value)
    {
      throw new UnsupportedOperationException();
    }    
  } 
}

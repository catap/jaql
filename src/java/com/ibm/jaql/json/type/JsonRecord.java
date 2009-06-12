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

import com.ibm.jaql.util.BaseUtil;

/**
 * 
 */
public abstract class JsonRecord extends JsonValue implements Iterable<Entry<JsonString, JsonValue>>
{
  public static final JsonRecord EMPTY = new BufferedJsonRecord(); // TODO: should be immutable

  /**
   * @return
   */
  public abstract int arity();
  /**
   * @param name
   * @return
   */
  public abstract int findName(JsonString name);
  /**
   * @param name
   * @return
   */
  public abstract int findName(String name);
  /**
   * @param i
   * @return
   */
  public abstract JsonString getName(int i);
  
  /**
   * @param i
   * @return
   */
  public abstract JsonValue getValue(int i);

  
  /**
   * @param name
   * @param dfault
   * @return
   */
  public JsonValue getValue(JsonString name, JsonValue defaultValue)
  {
    int i = findName(name);
    if (i < 0)
    {
      return defaultValue;
    }
    return getValue(i);
  }

  /**
   * @param name
   * @param dfault
   * @return
   */
  public JsonValue getValue(String name, JsonValue defaultValue)
  {
    int i = findName(name);
    if (i < 0)
    {
      return defaultValue;
    }
    return getValue(i);
  }

  /**
   * @param name
   * @return
   */
  public final JsonValue getValue(JsonString name)
  {
    return getValue(name, null);
  }

  /**
   * @param name
   * @return
   */
  public final JsonValue getValue(String name)
  {
    return getValue(name, null);
  }

  /**
   * @param name
   * @return
   */
  public final JsonValue getRequired(String name)
  {
    JsonValue value = getValue(name, null);
    if (value == null)
    {
      throw new RuntimeException("field '" + name + "' required");
    }
    return value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#compareTo(java.lang.Object)
   */
  public final int compareTo(Object arg0)
  {
    // TODO: this should be order-independent
    JsonRecord t = (JsonRecord) arg0;
    int cmp;
    int a1 = arity();
    int a2 = t.arity();
    int end = Math.min(a1, a2);
    for (int i = 0; i < end; i++)
    {
      JsonString n1 = getName(i);
      JsonString n2 = t.getName(i);
      cmp = n1.compareTo(n2);
      if (cmp != 0)
      {
        return cmp;
      }
      JsonValue v1 = getValue(i);
      JsonValue v2 = t.getValue(i);
      cmp = v1.compareTo(v2);
      if (cmp != 0)
      {
        return cmp;
      }
    }
    if (a1 == a2)
    {
      return 0;
    }
    else if (a1 < a2)
    {
      return -1;
    }
    else
    // a1 > a2
    {
      return 1;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#longHashCode()
   */
  @Override
  public long longHashCode()
  {
    long h = BaseUtil.GOLDEN_RATIO_64;
    int arity = arity();
    for (int i = 0; i < arity; i++)
    {
      h |= getName(i).hashCode();
      h *= BaseUtil.GOLDEN_RATIO_64;
      h |= getValue(i).hashCode();
      h *= BaseUtil.GOLDEN_RATIO_64;
    }
    return (int) (h >> 32);
  }
  
  public Iterator<Entry<JsonString, JsonValue>> iterator()
  {
    return new Iterator<Entry<JsonString, JsonValue>>()
    {
      int i = 0;
      
      @Override
      public boolean hasNext()
      {
        return i < arity();
      }

      @Override
      public Entry<JsonString, JsonValue> next()
      {
        Entry<JsonString, JsonValue> result = 
          new RecordEntry(getName(i), getValue(i));
        i++;
        return result;
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException();        
      }      
    };
  }

  //  public boolean containsKey(JString key)
  //  {
  //    return getValue(key, null) != null;
  //  }
  //
  //  public boolean containsKey(Object x)
  //  {
  //    return containsKey((JString)x);
  //  }
  //
  //  public boolean containsValue(Item value)
  //  {
  //    for( int i = 0 ; i < arity ; i++ )
  //    {
  //      if( entries[i].value.equals(value) )
  //      {
  //        return true;
  //      }
  //    }
  //    return false;
  //  }
  //
  //  public boolean containsValue(Object x)
  //  {
  //    return containsValue((Item)x);
  //  }
  //
  //  public Item get(Object name)
  //  {
  //    return get((JString)name);
  //  }
  //
  //  public boolean isEmpty()
  //  {
  //    return arity == 0;
  //  }
  //
  //  public Item put(JString name, Item value)
  //  {
  //    return null;
  //  }
  //
  //  public void putAll(Map<? extends JString, ? extends Item> arg0)
  //  {
  //    // TODO Auto-generated method stub
  //    
  //  }
  //
  //  public Item remove(JString key)
  //  {
  //    int i = findName(key);
  //    if( i < 0 )
  //    {
  //      return null;
  //    }
  //    Entry e = entries[i];
  //    arity--;
  //    entries[i] = entries[arity];
  //    entries[arity] = e;
  //    return e.value;
  //  }
  //  
  //  public Item remove(Object x)
  //  {
  //    return remove((JString)x);
  //  }
  //
  //  public int size()
  //  {
  //    return arity;
  //  }
  //
  //  public Set<JString> keySet()
  //  {
  //    // TODO Auto-generated method stub
  //    return null;
  //  }
  //
  //  public Collection<Item> values()
  //  {
  //    // TODO Auto-generated method stub
  //    return null;
  //  }
  //
  //  public Set<Map.Entry<JString, Item>> entrySet()
  //  {
  //    
  //    return null;
  //  }

  //  public void setVisible(int i, boolean visible)
  //  {
  //    entries[i].visible = visible;
  //  }
  
  private class RecordEntry implements Entry<JsonString, JsonValue>
  {
    private JsonString name;
    private JsonValue value;
    
    public RecordEntry(JsonString name, JsonValue value)
    {
      this.name = name;
      this.value = value;
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

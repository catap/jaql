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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import com.ibm.jaql.util.BaseUtil;

/**
 * 
 */
public class MemoryJRecord extends JRecord
{
  /**
   * 
   */
  public static class Entry implements Map.Entry<JString, Item>
  {
    protected JString name;
    protected Item    value;
    // boolean visible = true;

    public Entry()
    {
      name = new JString();
      value = new Item();
    }

    public Entry(JString name, Item value)
    {
      this.name = name;
      this.value = value;
    }

    public JString getKey()
    {
      return name;
    }

    public void setKey(JString name)
    {
      this.name = name;
    }

    public Item getValue()
    {
      return value;
    }

    public Item setValue(Item value)
    {
      Item old = this.value;
      this.value = value;
      return old;
    }
  }

  protected final static Entry[] NO_ENTRIES = new Entry[0];

  protected int                  arity      = 0;
  protected Entry[]              entries    = NO_ENTRIES;

  /**
   * 
   */
  public MemoryJRecord()
  {
  }

  /**
   * @param capacity
   */
  public MemoryJRecord(int capacity)
  {
    ensureCapacity(capacity);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  public Item.Encoding getEncoding()
  {
    return Item.Encoding.MEMORY_RECORD;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#arity()
   */
  public int arity()
  {
    return this.arity;
  }

  /**
   * @param arity
   */
  private void setArity(int arity)
  {
    if (entries.length < arity)
    {
      int len = entries.length;
      Entry[] e2 = new Entry[arity];
      System.arraycopy(entries, 0, e2, 0, len);
      for (int i = len; i < arity; i++)
      {
        e2[i] = new Entry();
      }
      entries = e2;
    }
    this.arity = arity;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#findName(com.ibm.jaql.json.type.JString)
   */
  public int findName(JString name)
  {
    // TODO: take advantage of sort order, if i keep it sorted
    for (int i = 0; i < arity; i++)
    {
      if (entries[i].name.equals(name))
      {
        return i;
      }
    }
    return -1;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#findName(java.lang.String)
   */
  public int findName(String name)
  {
    // TODO: take advantage of sort order, if i keep it sorted
    for (int i = 0; i < arity; i++)
    {
      if (entries[i].name.toString().equals(name))
      {
        return i;
      }
    }
    return -1;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#getName(int)
   */
  public JString getName(int i)
  {
    return entries[i].name;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#getValue(int)
   */
  public Item getValue(int i)
  {
    return entries[i].value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#getValue(com.ibm.jaql.json.type.JString,
   *      com.ibm.jaql.json.type.Item)
   */
  public Item getValue(JString name, Item dfault)
  {
    // TODO: take advantage of sort order, if i keep it sorted
    for (int i = 0; i < arity; i++)
    {
      if (entries[i].name.equals(name))
      {
        return entries[i].value;
      }
    }
    return dfault;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#getValue(java.lang.String,
   *      com.ibm.jaql.json.type.Item)
   */
  public Item getValue(String name, Item dfault)
  {
    // TODO: take advantage of sort order, if i keep it sorted
    JString nameText = new JString(name); // TODO: add JString/String compare
    for (int i = 0; i < arity; i++)
    {
      if (entries[i].name.equals(nameText))
      {
        return entries[i].value;
      }
    }
    return dfault;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException
  {
    int arity = BaseUtil.readVUInt(in);
    setArity(arity);
    for (int i = 0; i < arity; i++)
    {
      entries[i].name.readFields(in);
      entries[i].value.readFields(in);
      // entries[i].visible = true;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#write(java.io.DataOutput)
   */
  public void write(DataOutput out) throws IOException
  {
    //    int n = 0;
    //    for(int i = 0 ; i < arity ; i++ )
    //    {
    //      if( entries[i].visible )
    //      {
    //        n++;
    //      }
    //    }
    BaseUtil.writeVUInt(out, arity);
    for (int i = 0; i < arity; i++)
    {
      //      if( entries[i].visible )
      //      {
      entries[i].name.write(out);
      entries[i].value.write(out);
      //      }
    }
  }

  /**
   * 
   */
  public void clear()
  {
    this.arity = 0;
  }

  //  public void unsafeAdd(JString name, Item value)
  //  {
  //    int i = arity;
  //    setArity(i + 1);
  //    entries[i].name = name; // TODO: unsafe to call readfields now because we don't own this name
  //    set(i, value);
  //  }

  /**
   * name and value now belong to this record.
   * 
   * @param name
   * @param value
   */
  public void add(JString name, Item value)
  {
    // TODO: take advantage of sort order, if i keep it sorted
    int i;
    for (i = 0; i < arity; i++)
    {
      int c = entries[i].name.compareTo(name);
      if (c == 0)
      {
        throw new RuntimeException("duplicate field name: " + name);
      }
      if (c > 0)
      {
        break;
      }
    }
    setArity(arity + 1);
    Entry e = entries[arity - 1];
    System.arraycopy(entries, i, entries, i + 1, arity - i - 1);
    entries[i] = e;
    e.name = name; // TODO: unsafe to call readfields now because we don't own this name
    set(i, value);
  }

  //  public void add(JString name, Item value)
  //  {
  //    if( getValue(name, null) != null )
  //    {
  //      throw new RuntimeException("duplicate field name: "+name);
  //    }
  //    unsafeAdd(name, value);
  //  }

  /**
   * @param name
   * @param value
   */
  public void add(String name, Item value)
  {
    add(new JString(name), value); // TODO: memory
  }

  /**
   * @param name
   * @param value
   */
  public void add(JString name, JValue value)
  {
    add(name, new Item(value)); // TODO: memory
  }

  /**
   * @param name
   * @param value
   */
  public void add(String name, JValue value)
  {
    add(name, new Item(value)); // TODO: memory
  }

  /**
   * @param i
   * @param value
   */
  public void set(int i, Item value)
  {
    //    if( value == null || value == Item.empty )
    //    {
    //      entries[i].visible = false; // TODO: automatic invisiblity of null values?
    //    }
    //    else
    //    {
    entries[i].value = value; // TODO: unsafe to call readfields now because we don't own this value
    //      entries[i].visible = true;
    //    }
  }

  /**
   * @param i
   * @param value
   */
  public void set(int i, JValue value)
  {
    set(i, new Item(value)); // TODO: memory
  }

  /**
   * @param name
   * @param value
   */
  public void set(JString name, Item value)
  {
    // TODO: take advantage of sort order, if i keep it sorted
    for (int i = 0; i < arity; i++)
    {
      if (entries[i].name.equals(name))
      {
        set(i, value);
        return;
      }
    }
    // unsafeAdd(name, value);
    add(name, value);
  }

  /**
   * @param name
   * @param value
   */
  public void set(String name, Item value)
  {
    // TODO: take advantage of sort order, if i keep it sorted
    JString nameText = new JString(name); // TODO: add JString.equals(String)
    for (int i = 0; i < arity; i++)
    {
      if (entries[i].name.equals(nameText))
      {
        set(i, value);
        return;
      }
    }
    // unsafeAdd(nameText, value);
    add(nameText, value);
  }

  /**
   * @param name
   * @param value
   */
  public void set(JString name, JValue value)
  {
    set(name, new Item(value)); // TODO: memory
  }

  /**
   * @param name
   * @param value
   */
  public void set(String name, JValue value)
  {
    set(name, new Item(value)); // TODO: memory
  }

  /**
   * @return
   */
  public int getCapacity()
  {
    return entries.length;
  }

  /**
   * @param capacity
   */
  public void ensureCapacity(int capacity)
  {
    if (entries.length < capacity)
    {
      Entry[] e = new Entry[capacity];
      System.arraycopy(entries, 0, e, 0, entries.length);
      for (int i = entries.length; i < capacity; i++)
      {
        e[i] = new Entry();
      }
      entries = e;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#hashCode()
   */
  public int hashCode()
  {
    long h = BaseUtil.GOLDEN_RATIO_64;
    for (int i = 0; i < arity; i++)
    {
      h |= entries[i].name.hashCode();
      h *= BaseUtil.GOLDEN_RATIO_64;
      h |= entries[i].value.hashCode();
      h *= BaseUtil.GOLDEN_RATIO_64;
    }
    return (int) (h >> 32);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#equals(java.lang.Object)
   */
  public boolean equals(Object x)
  {
    return this.compareTo(x) == 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#copy(com.ibm.jaql.json.type.JValue)
   */
  public void copy(JValue value) throws Exception
  {
    JRecord r = (JRecord) value;
    int n = r.arity();
    ensureCapacity(n);
    arity = n;
    for (int i = 0; i < n; i++)
    {
      entries[i].name.copy(r.getName(i));
      entries[i].value.copy(r.getValue(i));
    }
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
}

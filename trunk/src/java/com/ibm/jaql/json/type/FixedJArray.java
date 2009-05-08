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

import com.ibm.jaql.json.util.Iter;

/*
 * This is a fixed-sized array (i.e., a tuple). It is used when the length of
 * the array is known at in advance and does not vary. It does NOT copy the
 * items placed inside of it.
 * 
 * This array class does NOT copy items added to it; the items are referenced.
 * Therefore, it is crucial that the items remain valid for the lifetime of this
 * array.
 * 
 */
public final class FixedJArray extends JArray
{
  public final static Item[] NO_ITEMS = new Item[0];

  /**
   * @return
   */
  public static FixedJArray getEmpty()
  {
    if (EMPTY != null)
    {
      return EMPTY;
    }
    return new FixedJArray(new Item[0]);
  }

  protected Item[] items;
  protected int    count;

  /**
   * @param items
   */
  public FixedJArray(Item[] items)
  {
    this.items = items;
    this.count = items.length;
  }

  /**
   * @param size
   */
  public FixedJArray(int size)
  {
    this(new Item[size]);
  }

  /**
   * 
   */
  public FixedJArray()
  {
    this(NO_ITEMS);
  }

  /**
   * 
   */
  public void clear()
  {
    count = 0;
  }

  /**
   * @param size
   */
  public final void ensureCapacity(int size)
  {
    if (size > items.length)
    {
      Item[] newItems = new Item[size];
      System.arraycopy(this.items, 0, newItems, 0, items.length);
      this.items = newItems;
    }
  }

  /**
   * @param size
   */
  public void resize(int size)
  {
    ensureCapacity(size);
    this.count = size;
  }

  /**
   * @param size
   */
  public void resize(long size)
  {
    resize((int) size);
  }

  /**
   * Same as count() except that it returns an int.
   * 
   * @return
   */
  public final int size()
  {
    return count;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#count()
   */
  public final long count()
  {
    return count;
  }

  /**
   * @param i
   * @return
   */
  public Item get(int i)
  {
    assert i < count;
    return items[i];
  }

  /**
   * @param i
   * @param item
   */
  public void set(int i, Item item)
  {
    assert i < count;
    assert item != null;
    items[i] = item;
  }

  /**
   * @param i
   * @param value
   */
  public void set(int i, JValue value)
  {
    if (items[i] == null)
    {
      items[i] = new Item();
    }
    items[i].set(value);
  }

  /**
   * @param item
   */
  public void add(Item item)
  {
    ensureCapacity(count + 1);
    items[count] = item;
    count++;
  }

  /**
   * @param value
   */
  public void add(JValue value)
  {
    ensureCapacity(count + 1);
    if (items[count] == null)
    {
      items[count] = new Item();
    }
    items[count].set(value);
    count++;
  }

  /**
   * @param x
   * @return
   */
  public final int compareTo(FixedJArray x)
  {
    int n = count;
    if (x.count < n)
    {
      n = x.count;
    }
    for (int i = 0; i < n; i++)
    {
      int c = items[i].compareTo(x.items[i]);
      if (c != 0)
      {
        return c;
      }
    }
    if (count == x.count)
    {
      return 0;
    }
    else if (count < x.count)
    {
      return +1;
    }
    else
    {
      return -1;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(Object x)
  {
    if (x instanceof FixedJArray)
    {
      return compareTo((FixedJArray) x);
    }
    return super.compareTo(x);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#copy(com.ibm.jaql.json.type.JValue)
   */
  @Override
  public void setCopy(JValue value) throws Exception
  {
    FixedJArray arr = (FixedJArray) value;
    int n = arr.count;
    resize(n);
    for (int i = 0; i < n; i++)
    {
      if (items[i] == null)
      {
        items[i] = new Item();
      }
      items[i].setCopy(arr.items[i]);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#getTuple(com.ibm.jaql.json.type.Item[])
   */
  @Override
  public void getTuple(Item[] tuple) throws Exception
  {
    assert tuple.length == count;
    System.arraycopy(items, 0, tuple, 0, count);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#longHashCode()
   */
  @Override
  public long longHashCode()
  {
    long h = initLongHash();
    Item[] items = this.items;
    for (int i = 0; i < count; i++)
    {
      h = longHashItem(h, items[i]);
    }
    return h;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#iter()
   */
  @Override
  public Iter iter() throws Exception
  {
    if (count == 0)
    {
      return Iter.empty;
    }
    return new Iter() {
      int i = 0;

      @Override
      public Item next() throws Exception
      {
        if (i < count)
        {
          Item item = items[i];
          i++;
          return item;
        }
        return null;
      }
    };
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#nth(long)
   */
  @Override
  public Item nth(long n) throws Exception
  {
    if (n >= 0 && n < count)
    {
      return items[(int) n];
    }
    return Item.NIL;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  public Item.Encoding getEncoding()
  {
    return Item.Encoding.ARRAY_FIXED;
  }
}

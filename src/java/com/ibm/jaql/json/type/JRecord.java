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

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.UndeclaredThrowableException;

import com.ibm.jaql.util.BaseUtil;

/**
 * 
 */
public abstract class JRecord extends JValue
{
  public static final JRecord empty = new MemoryJRecord();

  /**
   * @return
   */
  public abstract int arity();
  /**
   * @param name
   * @return
   */
  public abstract int findName(JString name);
  /**
   * @param name
   * @return
   */
  public abstract int findName(String name);
  /**
   * @param i
   * @return
   */
  public abstract JString getName(int i);
  /**
   * @param i
   * @return
   */
  public abstract Item getValue(int i);

  /**
   * @param name
   * @param dfault
   * @return
   */
  public Item getValue(JString name, Item dfault)
  {
    int i = findName(name);
    if (i < 0)
    {
      return dfault;
    }
    return getValue(i);
  }

  /**
   * @param name
   * @param dfault
   * @return
   */
  public Item getValue(String name, Item dfault)
  {
    int i = findName(name);
    if (i < 0)
    {
      return dfault;
    }
    return getValue(i);
  }

  /**
   * @param i
   * @return
   */
  public final JValue getJValue(int i)
  {
    return getValue(i).get();
  }

  /**
   * @param name
   * @return
   */
  public final Item getValue(JString name)
  {
    return getValue(name, Item.NIL);
  }

  /**
   * @param name
   * @return
   */
  public final Item getValue(String name)
  {
    return getValue(name, Item.NIL);
  }

  /**
   * @param name
   * @return
   */
  public final Item getRequired(String name)
  {
    Item item = getValue(name, null);
    if (item == null)
    {
      throw new RuntimeException("field " + name + " required");
    }
    return item;
  }

  /**
   * @param name
   * @return
   */
  public final JValue getNull(JString name)
  {
    Item x = getValue(name, null);
    if (x != null)
    {
      return x.getNonNull();
    }
    return null;
  }

  /**
   * @param name
   * @return
   */
  public final JValue getNull(String name)
  {
    Item x = getValue(name, null);
    if (x != null)
    {
      return x.getNonNull();
    }
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#readFields(java.io.DataInput)
   */
  public abstract void readFields(DataInput in) throws IOException;
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#write(java.io.DataOutput)
   */
  public abstract void write(DataOutput out) throws IOException;

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#compareTo(java.lang.Object)
   */
  public final int compareTo(Object arg0)
  {
    // TODO: this should be order-independent
    JRecord t = (JRecord) arg0;
    int cmp;
    int a1 = arity();
    int a2 = t.arity();
    int end = Math.min(a1, a2);
    for (int i = 0; i < end; i++)
    {
      JString n1 = getName(i);
      JString n2 = t.getName(i);
      cmp = n1.compareTo(n2);
      if (cmp != 0)
      {
        return cmp;
      }
      Item v1 = getValue(i);
      Item v2 = t.getValue(i);
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
   * @see com.ibm.jaql.json.type.JValue#print(java.io.PrintStream)
   */
  @Override
  public void print(PrintStream out) throws Exception
  {
    print(out, 0);
  }

  /**
   * 
   * <no indent> { <indent+2> name: value, ... <indent> }
   * 
   * OR
   * 
   * <no indent> {}
   * 
   * @param out
   * @param indent
   * @throws Exception
   */
  public void print(PrintStream out, int indent) throws Exception
  {
    out.print("{");
    indent += 2;
    final int arity = arity();
    String sep = "";
    for (int i = 0; i < arity; i++)
    {
      out.println(sep);
      for (int s = 0; s < indent; s++)
        out.print(' ');
      getName(i).print(out);
      out.print(": ");
      getValue(i).print(out, indent);
      sep = ",";
    }
    if (sep.length() > 0) // if not empty record
    {
      out.println();
      for (int s = 2; s < indent; s++)
        out.print(' ');
    }
    out.print("}");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#toJSON()
   */
  @Override
  public String toJSON()
  {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(os);
    try
    {
      this.print(ps);
    }
    catch (Exception e)
    {
      throw new UndeclaredThrowableException(e);
    }
    ps.flush();
    String s = os.toString();
    return s;
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

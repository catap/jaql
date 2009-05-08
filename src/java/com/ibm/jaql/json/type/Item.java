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
import java.util.HashMap;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.ibm.jaql.io.serialization.FullSerializer;
import com.ibm.jaql.json.util.ItemWritableComparator;
import com.ibm.jaql.lang.core.JFunction;

/** Encapsulates an arbitrary, single JSON value (instance of {@link JValue}). */
public final class Item implements WritableComparable<Item> // , Cloneable
{
  // The order listed here is the order that types compare
  public static enum Type
  {
    UNKNOWN(null, ""), // bogus item type used as an indicator
    // UNDEFINED(null, null), // reserved for possible inclusion of the undefined value
    NULL(null, "null"), 
    ARRAY(JArray.class, "array"), 
    RECORD(JRecord.class, "record"),
    BOOLEAN(JBool.class, "boolean"),
    STRING(JString.class, "string"),
    NUMBER(JNumber.class, "number"),

    // JSON extensions

    BINARY(JBinary.class, "binary"),
    DATE(JDate.class, "date"),
    SCHEMA(JSchema.class, "type"),
    FUNCTION(JFunction.class, "function"),

    // Extensiblity for writable java objects, but the class name is written on every instance!

    JAVAOBJECT(JJavaObject.class, "javaObject"), // extend by any writable object

    // Experimental types - They might disappear!

    REGEX(JRegex.class, "regex"), SPAN(JSpan.class, "span"), DOUBLE(
        JDouble.class, "double");

    private static final HashMap<String, Type>  nameToType  = new HashMap<String, Type>();
    private static final HashMap<JString, Type> jnameToType = new HashMap<JString, Type>();

    static
    {
      for (Type t : values())
      {
        nameToType.put(t.name, t);
        jnameToType.put(t.nameValue, t);
      }
    }

    public final Class<? extends JValue>        clazz;
    public final String                         name;
    public final JString                        nameValue;
    //public final Item                           nameItem;

    Type(Class<? extends JValue> clazz, String name)
    {
      this.clazz = clazz;
      this.name = name;
      this.nameValue = new JString(name);
      // BUG: this is a circular dependency, i.e., Item -> Encoding -> Type -> Item
      //this.nameItem = new Item(nameValue);
    }

    public static Type getType(String name)
    {
      return nameToType.get(name);
    }

    public static Type getType(JString name)
    {
      return jnameToType.get(name);
    }
  }

  // These IDs are stored on disk!
  // *** CHANGE WITH GREAT CARE ***
  public static enum Encoding
  {
    UNKNOWN(0, null, Type.UNKNOWN), // bogus item type used as an indicator
    // UNDEFINED(1, null, null), // reserved for possible inclusion of the undefined value
    NULL(2, null, Type.NULL),
    ARRAY_SPILLING(3, SpillJArray.class, Type.ARRAY),
    ARRAY_FIXED(4, FixedJArray.class, Type.ARRAY),
    MEMORY_RECORD(5, MemoryJRecord.class, Type.RECORD),
    BOOLEAN(6, JBool.class, Type.BOOLEAN),
    STRING(7, JString.class, Type.STRING),
    BINARY(8, JBinary.class, Type.BINARY),
    LONG(9, JLong.class, Type.NUMBER),
    DECIMAL(10, JDecimal.class, Type.NUMBER),
    DATE_MSEC(11, JDate.class, Type.DATE),
    FUNCTION(12, JFunction.class, Type.FUNCTION),
    SCHEMA(13, JSchema.class, Type.SCHEMA),
    JAVAOBJECT_CLASSNAME(14, JJavaObject.class, Type.JAVAOBJECT), // extension type that lists class name next
    REGEX(15, JRegex.class, Type.REGEX),
    SPAN(16, JSpan.class, Type.SPAN),
    DOUBLE(17, JDouble.class, Type.DOUBLE),
    JAVA_RECORD(18, JavaJRecord.class, Type.RECORD), 
    JAVA_ARRAY(19, JavaJRecord.class, Type.ARRAY);

    public final static int                                        LIMIT        = 20;                                             // keep at max id + 1
    private static final Encoding[]                                idToEncoding = new Encoding[LIMIT];
    private static final HashMap<Class<? extends JValue>, Integer> classMap     = new HashMap<Class<? extends JValue>, Integer>();

    public final int                                               id;
    public final Class<? extends JValue>                           clazz;
    public final Type                                              type;

    static
    {
      for (Encoding e : values())
      {
        idToEncoding[e.id] = e;
        classMap.put(e.clazz, e.id);
      }
    }

    Encoding(int id, Class<? extends JValue> clazz, Type type)
    {
      assert type != null;
      this.id = id;
      this.clazz = clazz;
      this.type = type;
      // classMap.put(clazz, this);
    }

    public static Encoding valueOf(int id)
    {
      return idToEncoding[id];
    }

    public JValue newInstance()
    {
      try
      {
        return (JValue) clazz.newInstance();
      }
      catch (InstantiationException e)
      {
        throw new RuntimeException(clazz.getName(), e);
      }
      catch (IllegalAccessException e)
      {
        throw new RuntimeException(clazz.getName(), e);
      }
    }

    public Type getType()
    {
      return type;
    }
  }

  public static final Item   NIL      = new Item();
  public static final Item[] NO_ITEMS = new Item[0];

  static
  {
    // register my comparator
    WritableComparator.define(Item.class, new ItemWritableComparator());
  }

  private Encoding           encoding;
  private JValue             value;
  private JValue             cache;                 // cache is the most recent non-null value (value == cache when value != null)

  /**
   * @param x
   * @param y
   * @return
   */
  public static int typeCompare(Item x, Item y)
  {
    return x.encoding.type.compareTo(y.encoding.type);
  }

  /**
   * 
   */
  public Item()
  {
    this.encoding = Encoding.NULL;
  }

  /** Does not copy.
   * @param v
   */
  public Item(JValue v)
  {
    set(v);
  }

  /**
   * @return
   */
  public boolean isNull()
  {
    return (value == null);
  }

  /**
   * @return
   */
  public Encoding getEncoding()
  {
    return encoding;
  }

  /**
   * @return
   */
  public Type getType()
  {
    return encoding.getType();
  }

  /**
   * @return
   */
  public JValue getNonNull()
  {
    if (value == null)
    {
      throw new NullPointerException("value must be non-null");
    }
    return value;
  }

  /** Returns the JValue represented by this item
   * @return
   */
  public JValue get() // TODO: delete me
  {
    return value;
  }

  /**
   * @return
   */
  public JValue restoreCache()
  {
    value = cache;
    return value;
  }

  /**
   * Set the value of this item to <code>v</code> without copying.
   * 
   * @param v a value
   */
  public void set(JValue v)
  {
    // TODO: cache old value.  save value when type is null?
    if (v == null) {
      encoding = Encoding.NULL;
      value = null;      
    } else {
      encoding = v.getEncoding();
      cache = value = v;
    }
  }

  /** Set the value of this item to a copy of <code>v</code>. 
   * 
   * @param v a value
   * @throws Exception
   */
  public void setCopy(JValue v) throws Exception 
  {
    if (v == null) {
      encoding = Encoding.NULL;
      value = null;
    } else {
      if (encoding != v.getEncoding() || value==v)
      {
        encoding = v.getEncoding();
        cache = value = encoding.newInstance();
      }
      value.setCopy(v);
    }
  }

  /** Copy the content of <code>item</code> into this object. 
   * 
   * @param item an item
   * @throws Exception
   */
  public void setCopy(Item item) throws Exception
  {
    if (item == null) {
      encoding = Encoding.NULL;
      value = null;
      return;
    }
    setCopy(item.get());
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException
  {
    cache = value = FullSerializer.getDefault().read(in, value);
    if (value != null)
    {
      encoding = value.getEncoding();
    } else {
      encoding = Encoding.NULL;
    }
  }

  public void write(DataOutput out) throws IOException
  {
    FullSerializer.getDefault().write(out, value);
  }
  
  
//  private static class BufferedOutputStream extends OutputStream {
//    static final int BUF_SIZE = 1024;
//    byte[] buf = new byte[BUF_SIZE];
//    int count = 0;
//    DataOutput out;
//    
//    public void setOut(DataOutput out) {
//      assert count==0;
//      this.out = out;
//    }
//    
//    @Override
//    public void write(int b) throws IOException
//    {
//      if (count==buf.length) {
//        flush();
//      }
//      buf[count] = (byte)b;
//      count++;      
//    }
//    
//    public void write(byte b[], int off, int len)  throws IOException {
//      // does not fit
//      if (count+len>buf.length) {
//        flush(); // TODO: could be optimized
//        if (len>=buf.length) {
//          out.write(b, off, len);
//          return;
//        }
//      }
//      
//      // fits
//      System.arraycopy(b, off, buf, count, len);
//      count += len;
//    }
//    
//    public void flush() throws IOException {
//      if (count > 0) {
//        try
//        {
//          out.write(buf, 0, count);
//        } catch (IOException e)
//        {
//          throw new RuntimeException(e);
//        }
//        count=0;
//      }
//    }
//  }
//  
//  private static final BufferedOutputStream OUTPUT_STREAM = new BufferedOutputStream();
//  private static final DataOutputStream DATA_OUTPUT = new DataOutputStream(OUTPUT_STREAM);
//
//  
//  /*
//   * (non-Javadoc)
//   * 
//   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
//   */
//  public void write(DataOutput out) throws IOException
//  {
//    if (out != DATA_OUTPUT) { // make sure branch is executed only once
//      OUTPUT_STREAM.setOut(out);
//      BaseUtil.writeVUInt(DATA_OUTPUT, encoding.id);
//      if (value != null)
//      {
//        value.write(DATA_OUTPUT);
//      }
//      else
//      {
//        assert encoding == Encoding.NULL;
//      }
//      OUTPUT_STREAM.flush();
//    } else {
//      BaseUtil.writeVUInt(out, encoding.id);
//      if (value != null)
//      {
//        value.write(out);
//      }
//      else
//      {
//        assert encoding == Encoding.NULL;
//      }      
//    }
//  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#equals(java.lang.Object)
   */
  public boolean equals(Object x)
  {
    return this.compareTo((Item)x) == 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  public int compareTo(Item x)
  {
    int cmp = typeCompare(this, x);
    if (cmp != 0)
    {
      return cmp;
    }
    if (value == null)
    {
      assert encoding == Encoding.NULL;
      return 0;
    }
    return value.compareTo(x.value);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode()
  {
    if (value == null)
    {
      assert encoding == Encoding.NULL;
      return 0;
    }
    return value.hashCode();
  }

  /**
   * @return
   */
  public long longHashCode()
  {
    if (value == null)
    {
      assert encoding == Encoding.NULL;
      return 0;
    }
    return value.longHashCode();
  }

  /**
   * @return
   */
  public String toJSON()
  {
    if (value == null)
    {
      return "null";
    }
    else
    {
      return value.toJSON();
    }
  }

  /**
   * @param out
   * @throws Exception
   */
  public void print(PrintStream out) throws Exception
  {
    if (value == null)
    {
      out.print("null");
    }
    else
    {
      value.print(out);
    }
  }

  /**
   * @param out
   * @param indent
   * @throws Exception
   */
  public void print(PrintStream out, int indent) throws Exception
  {
    if (value == null)
    {
      out.print("null");
    }
    else
    {
      value.print(out, indent);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    try
    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream out = new PrintStream(baos);
      this.print(out, 0);
      out.flush();
      return baos.toString();
    }
    catch (Exception e)
    {
      return "exception()";
    }
  }

  /**
   * @return
   */
  final public boolean isAtom()
  {
    return !(value instanceof JRecord || value instanceof JArray);
  }
  
  /** Reset the content of this item. After resetting, the state of this item is identical 
   * to the state of a newly created item. All internal values and cache elements referenced by 
   * this item are not touched; but the reseted item does not reference them anymore.
   */
  public void reset() {
    encoding = Encoding.NULL;
    value = null;
    cache = null;
  }
}

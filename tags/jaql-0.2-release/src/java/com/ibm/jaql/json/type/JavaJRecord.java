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

import com.ibm.jaql.json.meta.MetaRecord;
import com.ibm.jaql.json.type.Item.Encoding;

/**
 * 
 */
public class JavaJRecord extends JRecord
{
  protected MetaRecord meta;
  protected Object     value;
  protected Item[]     fieldBuffers;

  /**
   * 
   */
  public JavaJRecord()
  {
  }

  /**
   * @param value
   */
  public JavaJRecord(Object value)
  {
    setObject(value);
  }

  /**
   * @param value
   */
  public void setObject(Object value)
  {
    if (meta == null || meta.getClazz() != value.getClass())
    {
      meta = MetaRecord.getMetaRecord(value.getClass());
      fieldBuffers = meta.makeItems();
    }
    this.value = value;
  }

  /**
   * @return
   */
  public Object getObject()
  {
    return value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#arity()
   */
  @Override
  public int arity()
  {
    return fieldBuffers.length;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#findName(com.ibm.jaql.json.type.JString)
   */
  @Override
  public int findName(JString name)
  {
    return meta.findField(name);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#findName(java.lang.String)
   */
  @Override
  public int findName(String name)
  {
    return meta.findField(name);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#getName(int)
   */
  @Override
  public JString getName(int i)
  {
    return meta.getName(i);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#getValue(int)
   */
  @Override
  public Item getValue(int i)
  {
    Item item = fieldBuffers[i];
    meta.getValue(value, i, item);
    return item;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException
  {
    String className = in.readUTF(); // TODO: would like to compress out the class name...
    if (meta == null || !meta.getClazz().getName().equals(className))
    {
      meta = MetaRecord.getMetaRecord(className);
      value = meta.newInstance();
      fieldBuffers = meta.makeItems();
    }
    value = meta.read(in, value);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException
  {
    out.writeUTF(meta.getClazz().getName()); // TODO: would like to compress out the class name...
    meta.write(out, value);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#copy(com.ibm.jaql.json.type.JValue)
   */
  @Override
  public void copy(JValue jvalue) throws Exception
  {
    JavaJRecord that = (JavaJRecord) jvalue;
    meta.copy(this.value, that.value);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  @Override
  public Encoding getEncoding()
  {
    return Item.Encoding.JAVA_RECORD;
  }

  /**
   * 
   */
  public static class Test
  {
    public long       n;
    public long       m          = 17;
    protected long    no;

    public boolean    xbool      = true;
    public byte       xbyte      = 1;
    public char       xchar      = 'x';
    public short      xshort     = 3;
    public int        xint       = 4;
    public long       xlong      = 5;
    public double     xdouble    = 6.3;
    public float      xfloat     = 5.1f;
    public String     xstring    = "meta-Wow!";

    public Test       xnull      = null;
    public Test       nested;

    public long[]     xlongsNull = null;

    public boolean[]  xbools     = new boolean[]{true, false, true};
    public byte[]     xbytes     = new byte[]{3, 2, 1};
    public char[]     xchars     = new char[]{'c', 'b', 'a'};
    public short[]    xshorts    = new short[]{6, 5, 4};
    public int[]      xints      = new int[]{9, 8, 7};
    public long[]     xlongs     = new long[]{12, 11, 10};
    public double[]   xdoubles   = new double[]{6.6, 5.5, 4.4};
    public float[]    xfloats    = new float[]{3.3f, 2.2f, 1.1f};
    public String[]   xstrings   = new String[]{"three", "two", "one"};

    protected boolean ybool      = true;
    protected byte    ybyte      = 1;
    protected char    ychar      = 'y';
    protected short   yshort     = 3;
    protected int     yint       = 4;
    protected long    ylong      = 5;
    protected double  ydouble    = 6.3;
    protected float   yfloat     = 5.1f;
    protected String  ystring    = "y not?";
    protected Test    ynull      = null;
    protected Test    ynested;

    public Test()
    {
    }
    Test(long n)
    {
      this.xlong = n;
      ylong = -n;
    }

    public boolean getBool()
    {
      return ybool;
    }
    public void setBool(boolean x)
    {
      ybool = x;
    }
    public byte getByte()
    {
      return ybyte;
    }
    public void setByte(byte x)
    {
      ybyte = x;
    }
    public char getChar()
    {
      return ychar;
    }
    public void setChar(char x)
    {
      ychar = x;
    }
    public short getShort()
    {
      return yshort;
    }
    public void setShort(short x)
    {
      yshort = x;
    }
    public int getInt()
    {
      return yint;
    }
    public void setInt(int x)
    {
      yint = x;
    }
    public long getLong()
    {
      return ylong;
    }
    public void setLong(long x)
    {
      ylong = x;
    }
    public double getDouble()
    {
      return ydouble;
    }
    public void setDouble(double x)
    {
      ydouble = x;
    }
    public float getFloat()
    {
      return yfloat;
    }
    public void setFloat(float x)
    {
      yfloat = x;
    }
    public String getString()
    {
      return ystring;
    }
    public void setString(String x)
    {
      ystring = x;
    }
    //public Test    getNull()   { return ynull;   }  public void setNull(Test x) { ynull = x; }
    //public Test    getNested() { return ynested; }  public void setNested(Test x) { ynested = x; }

    protected String[] ystrings = new String[]{"a", "protected", "string",
                                    "array"};
    public String[] getStrings()
    {
      return ystrings;
    }
    public void setStrings(String[] x)
    {
      ystrings = x;
    }
  };
  /**
   * @param n
   * @return
   */
  public JavaJRecord eval(JNumber n)
  {
    Test test = new Test(n.longValue());
    test.nested = new Test(n.longValue() + 1);
    return new JavaJRecord(test);
  }
}

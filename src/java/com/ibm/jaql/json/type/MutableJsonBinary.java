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

/** An mutable atomic JSON value representing a byte array. */
public class MutableJsonBinary extends JsonBinary
{
  // -- construction ------------------------------------------------------------------------------
  
  /** Constructs an empty MutableJsonBinary. */
  public MutableJsonBinary()
  {
    super(EMPTY_BUFFER);
  }
  
  /** @see JsonBinary#JsonBinary(byte[]) */
  public MutableJsonBinary(byte[] bytes)
  {
    super(bytes);
  }

  /** @see JsonBinary#JsonBinary(byte[], int) */
  public MutableJsonBinary(byte[] bytes, int length)
  {
    super(bytes, length);        
  }

  /** @see JsonBinary#JsonBinary(byte[], int, int) */
  public MutableJsonBinary(byte[] bytes, int offset, int length)
  {
    super(bytes, offset, length);        
  }

  /** @see JsonBinary#JsonBinary(String) */
  public MutableJsonBinary(String hexString)
  {
    super(hexString);
  }

  /** Constructs a MutableJsonBinary from the provided byte array. If <code>copy</code> 
   * is set to false, the constructed string will be backed by <code>value</code>. 
   * Otherwise, a copy of <code>bytes</code> will be used. */
  public MutableJsonBinary(byte[] bytes, boolean copy)
  {
    this(bytes, bytes.length, copy);
  }

  /** Constructs a MutableJsonBinary from the provided byte array. If <code>copy</code> 
   * is set to false, the constructed string will be backed by <code>value</code>. 
   * Otherwise, a copy of <code>bytes</code> will be used. */
  public MutableJsonBinary(byte[] bytes, int len, boolean copy)
  {
    this();
    if (copy)
    {
      setCopy(bytes, len);
    }
    else
    {
      set(bytes, len);
    }
  }

  
  // -- reading/writing ---------------------------------------------------------------------------

  /** Returns the internal byte buffer. Modifications to this buffer will modify the value of
   * this binary. Use with caution! */
  public byte[] get()
  {
    return bytes;
  }

  /* @see com.ibm.jaql.json.type.JsonValue#getCopy(com.ibm.jaql.json.type.JsonValue) */
  @Override
  public MutableJsonBinary getCopy(JsonValue target) throws Exception
  {
    if (target == this) target = null;
    
    MutableJsonBinary t;
    if (target instanceof MutableJsonBinary)
    {
      t = (MutableJsonBinary)target;
    }
    else
    {
      t = new MutableJsonBinary();
    }
    t.setCopy(this.bytes, 0, this.bytesLength);
    return t;
  }
  
  /* @see com.ibm.jaql.json.type.JsonValue#getImmutableCopy() */
  @Override
  public JsonBinary getImmutableCopy() 
  {
    return new JsonBinary(bytes);
  }
  
  // -- mutation ----------------------------------------------------------------------------------
  
  // make public
  @Override
  public void set(byte[] bytes)
  {
    super.set(bytes);
  }

  // make public
  @Override
  public void set(byte[] bytes, int length)
  {
    super.set(bytes, length);
  }
  
  // make public
  @Override
  public void setCopy(byte[] bytes) {
    super.setCopy(bytes);
  }

  // make public
  @Override
  public void setCopy(byte[] bytes, int length) {
    super.setCopy(bytes, length);
  }
  
  // make public
  @Override
  public void setCopy(byte[] bytes, int pos, int length) {
    super.setCopy(bytes, pos, length);
  }
  
  // make public
  @Override
  public void ensureCapacity(int capacity)
  {
    super.ensureCapacity(capacity);
  }
}

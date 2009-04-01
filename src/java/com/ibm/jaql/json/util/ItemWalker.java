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
package com.ibm.jaql.json.util;

import java.io.DataInput;
import java.io.IOException;
import org.apache.hadoop.io.DataInputBuffer;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.util.BaseUtil;
import com.ibm.jaql.util.IntArray;

/** Iterator-like traversal over the serialized representation of a JSON value
 * 
 */
public class ItemWalker
{
  public final static int    END_RECORD     = -3;
  public final static int    END_ARRAY      = -2;
  public final static int    EOF            = -1;
  public final static int    NULL           = 0;
  public final static int    ATOM           = 1;
  public final static int    ARRAY          = 2;
  public final static int    RECORD         = 3;
  public final static int    FIELD_NAME     = 4;

  // Parsing States
  protected final static int S_ITEM         = 0;
  protected final static int S_NT_ARRAY     = 1;                              // Null-terminated array (Item.Encoding.Unknown)
  protected final static int S_COUNT_RECORD = 2;                              // remaining count is pushed before this state
  protected final static int S_COUNT_ARRAY  = 3;                              // remaining count is pushed before this state
  protected final static int S_TOP          = 4;

  protected IntArray         stack          = new IntArray();
  protected DataInput        input;
  protected DataInputBuffer  dataInputBuffer;
  protected JValue[]         atoms          = new JValue[Item.Encoding.LIMIT];

  public JString             name           = new JString();
  public JValue              atom;
  public Item                item           = new Item();
  public Item.Type           type;

  /**
   * 
   */
  public ItemWalker()
  {
  }

  /**
   * @param input
   */
  protected void reset(DataInput input)
  {
    this.input = input;
    stack.clear();
    stack.add(S_TOP);
    stack.add(S_ITEM);
  }

  /**
   * @param input
   */
  public void resetSpillArray(DataInput input)
  {
    this.input = input;
    stack.clear();
    stack.add(S_TOP);
    stack.add(S_NT_ARRAY);
  }

  /**
   * @param buffer
   * @param start
   * @param len
   */
  public void reset(byte[] buffer, int start, int len)
  {
    if (dataInputBuffer == null)
    {
      dataInputBuffer = new DataInputBuffer();
    }
    dataInputBuffer.reset(buffer, start, len);
    reset(dataInputBuffer);
  }

  /**
   * @return
   * @throws IOException
   */
  protected int readItem() throws IOException
  {
    int encId = BaseUtil.readVUInt(input);
    Item.Encoding enc = Item.Encoding.valueOf(encId);
    switch (enc)
    {
      case MEMORY_RECORD :
        int arity = BaseUtil.readVUInt(input);
        stack.add(arity);
        stack.add(S_COUNT_RECORD);
        atom = null;
        type = Item.Type.RECORD;
        return RECORD;

      case ARRAY_SPILLING :
        BaseUtil.readVULong(input); // read the array count (not used here)
        BaseUtil.readVULong(input); // read the array byte length
        stack.add(S_NT_ARRAY);
        atom = null;
        type = Item.Type.ARRAY;
        return ARRAY;

      case ARRAY_FIXED :
        int count = BaseUtil.readVUInt(input); // read the array count
        stack.add(count);
        stack.add(S_COUNT_ARRAY);
        atom = null;
        type = Item.Type.ARRAY;
        return ARRAY;

      case UNKNOWN : // end of array
        assert stack.top() == S_NT_ARRAY;
        stack.pop();
        atom = null;
        type = Item.Type.UNKNOWN;
        return END_ARRAY;

      case NULL :
        atom = null;
        type = Item.Type.NULL;
        return NULL;

      case JAVA_RECORD :
        // FIXME: There is a bug with the new JavaJRecord!!
        throw new RuntimeException("NYI");

      default :
        atom = atoms[encId];
        if (atom == null)
        {
          atom = atoms[encId] = enc.newInstance();
        }
        atom.readFields(input);
        type = enc.type;
        return ATOM;
    }
  }

  /** Returns type of next element: END_RECORD, END_ARRAY, EOF, NULL, ATOM, ARRAY, 
   * RECORD, FIELD_NAME
   * 
   * @return
   * @throws IOException
   */
  public int next() throws IOException
  {
    int n;

    switch (stack.pop())
    {
      case S_ITEM :
        return readItem();

      case S_NT_ARRAY :
        stack.add(S_NT_ARRAY);
        return readItem();

      case S_COUNT_RECORD :
        n = stack.pop();
        if (n == 0)
        {
          type = Item.Type.RECORD;
          return END_RECORD;
        }
        stack.add(n - 1);
        stack.add(S_COUNT_RECORD);
        stack.add(S_ITEM);
        name.readFields(input);
        atom = name;
        type = Item.Type.STRING;
        return FIELD_NAME;

      case S_COUNT_ARRAY :
        n = stack.pop();
        if (n == 0)
        {
          type = Item.Type.ARRAY;
          return END_ARRAY;
        }
        stack.add(n - 1);
        stack.add(S_COUNT_ARRAY);
        return readItem();

      case S_TOP :
        stack.add(S_TOP);
        type = Item.Type.UNKNOWN;
        return EOF;

      default :
        throw new RuntimeException("invalid state"); // We should never get here
    }
  }

  /** Returns the class of the current atom. Use only when {@link #next()} returned 
   * {@link #ATOM}.
   * 
   * @return
   */
  public Class<? extends JValue> getClazz()
  {
    return atom.getClass();
  }

  /** Returns the field name of current field. Use only when {@link #next()} returned 
   * {@link #FIELD_NAME}.
   * @return
   * @throws IOException
   */
  public JString getName() throws IOException
  {
    return name;
  }

  /** Returns the value of the current atom. Use only when {@link #next()} returned 
   * {@link #ATOM}. 
   * @return
   * @throws IOException
   */
  public JValue getAtom() throws IOException
  {
    return atom;
  }
}

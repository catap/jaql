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
import java.util.EnumMap;

import org.apache.hadoop.io.DataInputBuffer;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.type.Item.Encoding;
import com.ibm.jaql.lang.core.JComparator;
import com.ibm.jaql.util.BaseUtil;

/** Hadoop-compatible comparator for two JSON values. The comparator makes use of 
 * the total order of all types (to determine order of elements of different types). 
 * 
 * This class is not threadsafe. It can be used only with Hadoop version 0.18.0 and above because
 * earlier versions shared comparators between threads.
 */
public class ItemComparator implements JComparator
{
  // cache variables for hashing  
  protected DataInputBuffer buffer = new DataInputBuffer();
  protected Item key1 = new Item();
  
  // cache variables for comparing
  protected DataInputBuffer input1 = new DataInputBuffer();
  protected DataInputBuffer input2 = new DataInputBuffer();
  protected JString         name1  = new JString();
  protected JString         name2  = new JString();
  protected JValue[]        atoms1 = new JValue[Item.Encoding.LIMIT];
  protected JValue[]        atoms2 = new JValue[Item.Encoding.LIMIT];

  
  // -- raw comparators --------------------------------------------------------------------------

  /** Registry for raw comparators */
  private static final EnumMap<Item.Encoding, JRawComparator> COMPARATORS = 
    new EnumMap<Item.Encoding, JRawComparator>(Item.Encoding.class);
  
  /** Register a {@link JRawComparator} for the given <code>encoding</code>. */
  public static void define(Item.Encoding encoding, JRawComparator comparator) {
    COMPARATORS.put(encoding, comparator);
  }

  /** Superclass for comparators of values in their encoded form */  
  public interface JRawComparator {
    /** Compares the encoded value from <code>input1</code> with the encoded value from 
     * <code>input2</code>, where both inputs point to an atomic value of the same encoding. 
     * There should be one implementing class for each atomic encoding; see 
     * {@link ItemComparator#define(Encoding, JRawComparator)}. Comparision is
     * usually performed without decoding. This method (1) never reads more bytes 
     * than used by the encoded values, (2) does not read more bytes than necessary to make the 
     * decision, and (3) guarantees to read the entire encoded values in case of equality.   
     * 
     * @param input1 an input stream pointing to a value
     * @param input2 another input stream pointing to another value with the same encoding
     * @return
     * @throws IOException
     */
    public int compareValues(DataInput input1, DataInput input2) throws IOException;
  }
  
  
  // -- comparison -------------------------------------------------------------------------------

  /* @see org.apache.hadoop.io.WritableComparator#compare(byte[], int, int, byte[], int, int) */
  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
  {
    try
    {
      input1.reset(b1, s1, l1);
      input2.reset(b2, s2, l2);
      return compareItems(input1, input2);
    } catch (IOException e)
    {
      throw new RuntimeException(e);
    }
  }

  /* @see org.apache.hadoop.io.WritableComparator#compare(Object, Object) */
  @Override
  public int compare(Item a, Item b)
  {
    return a.compareTo(b);
  }

  /** Compares the encoded item from <code>input1</code> with the encoded item from 
   * <code>input2</code>. Comparison is usually performed without decoding. This method (1) never 
   * reads more bytes than used by the encoded items, (2) does not read more bytes than necessary 
   * to make the decision, and (3) guarantees to read the entire encoded items in case of 
   * equality.
   * 
   * @param input1 an input stream pointing to an item
   * @param input2 another input stream pointing to another item 
   * @return
   * @throws IOException
   */
  protected int compareItems(DataInput input1, DataInput input2) throws IOException {
    // read and compare encodings / types
    int code1 = BaseUtil.readVUInt(input1);
    int code2 = BaseUtil.readVUInt(input2);
    assert code1>0 && code2>0;
    Item.Encoding encoding1 = Item.Encoding.valueOf(code1);
    Item.Encoding encoding2 = Item.Encoding.valueOf(code2);
    if (encoding1 != encoding2) {
      Item.Type type1 = encoding1.getType();
      Item.Type type2 = encoding2.getType();
      int cmp = type1.compareTo(type2);
      if (cmp != 0) return cmp;

      // if same type but different encodings, deserialize
      // TODO: a better way / treat some cases special?
      return compareValuesDeserialized(input1, encoding1, input2, encoding2);
    }
    
    // compare values
    switch (encoding1)
    {
    case MEMORY_RECORD :  // JMemoryRecord
        int arity1 = BaseUtil.readVUInt(input1);
        int arity2 = BaseUtil.readVUInt(input2);
        return compareRecords(input1, arity1, input2, arity2);

      case ARRAY_SPILLING : // SpillJArray
        long lcount1 = BaseUtil.readVULong(input1);
        long lcount2 = BaseUtil.readVULong(input2);
        int cmp = compareArrays(input1, lcount1, input2, lcount2);
        if (cmp == 0) { // read sentinel in case of equality
          int term1 = BaseUtil.readVUInt(input1);
          int term2 = BaseUtil.readVUInt(input2);
          assert term1==Encoding.UNKNOWN.id && term2==Encoding.UNKNOWN.id;
        }
        return cmp;
        
      case ARRAY_FIXED :  // FixedJArray
        int icount1 = BaseUtil.readVUInt(input1); 
        int icount2 = BaseUtil.readVUInt(input2); 
        return compareArrays(input1, icount1, input2, icount2);

      case UNKNOWN : // sentinel, not a valid item!
        throw new IllegalStateException("Unknown encoding");

      case NULL: // null values
        return 0;

      case JAVA_RECORD :
        // FIXME: There is a bug with the new JavaJRecord!!
        throw new RuntimeException("NYI");

      default: // atomic values
        JRawComparator comparator = COMPARATORS.get(encoding1);
        if (comparator != null) {
          // no deserialization required
          return comparator.compareValues(input1, input2);
        } else {
          return compareValuesDeserialized(input1, encoding1, input2, encoding2);
        }
    }    
  }
  
  /** Compares the encoded value from <code>input1</code> with the encoded value from 
   * <code>input2</code> by deserializing. This method is used to compare types of different
   * encodings.   
   * 
   * @param input1 an input stream pointing to a value
   * @param input2 another input stream pointing to another value with the same encoding
   * @return
   * @throws IOException
   */
  protected int compareValuesDeserialized(DataInput input1, Item.Encoding encoding1, 
      DataInput input2, Item.Encoding encoding2) throws IOException 
  {
    // atoms can be overwritten; they are only used here 
    JValue value1 = atoms1[encoding1.id]; 
    if (value1 == null) {
      value1 = atoms1[encoding1.id] = encoding1.newInstance();
    }
    JValue value2 = atoms2[encoding2.id]; 
    if (value2 == null) {
      value2 = atoms2[encoding2.id] = encoding2.newInstance();
    }
    
    value1.readFields(input1);
    value2.readFields(input2);
    return value1.compareTo(value2);
  }
  
  /** Compares two encoded records of known arity without decoding them. This method (1) never 
   * reads more bytes than used by the encoded records, (2) does not read more bytes than necessary 
   * to make the decision, and (3) guarantees to read the entire encoded records in case of 
   * equality.  
   *  
   * @param input1 input stream pointing to the first element of the first record
   * @param count1 arity of first record
   * @param input2 input stream pointing to the first element of the second record
   * @param count2 arity of second record
   * @return
   * @throws IOException
   */
  protected int compareRecords(DataInput input1, int arity1, DataInput input2, int arity2) 
  throws IOException {
    int m = Math.min(arity1, arity2);
    for (int i=0; i<m; i++) {
      // names can be overwritten; they are only used here
      // TODO: binary comparison of JStrings
      name1.readFields(input1);
      name2.readFields(input2);
      int cmp = name1.compareTo(name2);
      if (cmp != 0) return cmp;
        
      // compare the values
      cmp = compareItems(input1, input2);
      if (cmp != 0) return cmp;
    }
    return arity1-arity2;
  }
  
  /** Compares two encoded item arrays of known length without decoding them. This method (1) never 
   * reads more bytes than used by the encoded arrays, (2) does not read more bytes than necessary 
   * to make the decision, and (3) guarantees to read the entire encoded arrays in case of 
   * equality.  
   *  
   * @param input1 input stream pointing to the first item of the first array
   * @param count1 length of first array
   * @param input2 input stream pointing to the first item of the second array
   * @param count2 length of second array
   * @return
   * @throws IOException
   */
  protected int compareArrays(DataInput input1, int count1, DataInput input2, int count2) 
  throws IOException {
    int m = Math.min(count1, count2);
    for (int i=0; i<m; i++) {
      int cmp = compareItems(input1, input2);
      if (cmp != 0) return cmp;
    }
    return count1-count2;  
  }
  
  /** Compares two encoded item arrays of known length without decoding them. This method (1) never 
   * reads more bytes than used by the encoded arrays, (2) does not read more bytes than necessary 
   * to make the decision, and (3) guarantees to read the entire encoded arrays in case of 
   * equality.  
   *  
   * @param input1 input stream pointing to the first item of the first array
   * @param count1 length of first array
   * @param input2 input stream pointing to the first item of the second array
   * @param count2 length of second array
   * @return
   * @throws IOException
   */
  protected int compareArrays(DataInput input1, long count1, DataInput input2, long count2) 
  throws IOException {
    long m = Math.min(count1, count2);
    for (long i=0; i<m; i++) {
      int cmp = compareItems(input1, input2);
      if (cmp != 0) return cmp;
    }
    return count1<count2 ? -1 : (count1==count2 ? 0 : 1);  
  }
  

  // -- hashing (currently unused) ---------------------------------------------------------------

  @Override
  public long longHash(byte[] bytes, int offset, int length) throws IOException
  {
    buffer.reset(bytes, offset, length);
    key1.readFields(buffer);
    longHash(key1);
    return 0;
  }

  @Override
  public long longHash(Item key)
  {
    return key1.longHashCode();
  }

}

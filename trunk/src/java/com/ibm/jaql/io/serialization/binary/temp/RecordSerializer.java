package com.ibm.jaql.io.serialization.binary.temp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.BaseUtil;

// not threadsafe
class RecordSerializer extends BinaryBasicSerializer<JsonRecord>
{
  // -- private variables -------------------------------------------------------------------------
  
  private RecordSchema schema;
  private TempBinaryFullSerializer nameSerializer;
  private TempBinaryFullSerializer additionalSerializer;
  
  // info variables
  private int noRequired;
  private int noOptional;
  private int noRequiredOptional;
  private FieldInfo[] requiredInfo;
  private FieldInfo[] optionalInfo; 
  private FieldInfo[] allInfo;
  
  // worker variables for serialization (content owned by someone else)
  private JsonValue[] requiredValues;       // values of required fields
  private MyBitSet optionalBits;            // bitmap indicating which optional fields are present
  private JsonValue[] optionalValues;       // values of the present optional fields
  private List<JsonString> additionalNames = new ArrayList<JsonString>(); // names of additional fields
  private List<JsonValue> additionalValues = new ArrayList<JsonValue>();  // values of additional fields
  
  // worker variables for binary comparison (content owned by this instance)
  private JsonString[] additionalNamesCache1;
  private JsonValue[] additionalValuesCache1;
  private JsonString[] additionalNamesCache2;
  private JsonValue[] additionalValuesCache2;
  private MyBitSet optionalBits1;
  private MyBitSet optionalBits2;
  
  /** Stores information about a required or optional field. */
  private static class FieldInfo
  {
    RecordSchema.Field field;
    TempBinaryFullSerializer serializer;
    int index; // index in requiredInfo or optionalInfo
    JsonString name;
    
    FieldInfo(RecordSchema.Field field, TempBinaryFullSerializer serializer)
    {
      this.field = field;
      this.serializer = serializer;
      this.name = (JsonString)field.getName().getCopy(null);
    }
  }
  
  /** Lightweight implementation of a bitset. */
  private static class MyBitSet
  {
    byte[] bytes;
    final static byte[] MASK = new byte[] { 1, 2, 4, 8, 16, 32, 64, (byte)128 };
    
    MyBitSet(int noBits)
    {
      bytes = new byte[(noBits+7)/8];
      clear();
    }
    
    void clear()
    {
      Arrays.fill(bytes, (byte)0);
    }
    
    public void set(int bit)
    {
      bytes[bit/8] |= MASK[bit % 8];
    }
    
    public boolean get(int bit)
    {
      return (bytes[bit/8] & MASK[bit % 8]) != (byte)0;
    }
  }
  
  /** Stores a name/value pair */
  private static class NameValue
  {
    JsonString name = null;
    JsonValue value = null;
  }
  
  // -- construction ------------------------------------------------------------------------------
  
  public RecordSerializer(RecordSchema schema)
  {
    this.schema = schema; 
    init();
  }
  
  private void init()
  {
    // create data structures
    noRequired = schema.noRequired();
    noOptional = schema.noOptional();
    noRequiredOptional = schema.noRequiredOrOptional();
    requiredInfo = new FieldInfo[noRequired];
    optionalInfo = new FieldInfo[noOptional];
    allInfo = new FieldInfo[noRequiredOptional];
    
    // scan required and optional fields
    for (int posRequired=0, posOptional=0, pos=0; pos < noRequiredOptional; pos++)
    {
      RecordSchema.Field field = schema.getField(pos);
      FieldInfo k = new FieldInfo(field, new TempBinaryFullSerializer(field.getSchema()));
      allInfo[pos] = k;
      if (field.isOptional()) {
        optionalInfo[posOptional] = k;
        k.index = posOptional;
        ++posOptional;
      }
      else
      {
        requiredInfo[posRequired] = k;
        k.index = posRequired;
        ++posRequired;
      }
    }
    
    // scan additional fields
    if (schema.getAdditionalSchema() != null)
    {
      additionalSerializer = new TempBinaryFullSerializer(schema.getAdditionalSchema());
      nameSerializer = new TempBinaryFullSerializer(SchemaFactory.stringSchema());
    }
    
    // intitialize worker variables
    optionalBits = new MyBitSet(noOptional);
    requiredValues = new JsonValue[noRequired];
    optionalValues = new JsonValue[noOptional];
    optionalBits1 = new MyBitSet(noOptional);
    optionalBits2 = new MyBitSet(noOptional);
    additionalNamesCache1 = new JsonString[0];
    additionalValuesCache1 = new JsonValue[0];
    additionalNamesCache2 = new JsonString[0];
    additionalValuesCache2 = new JsonValue[0]; 
  }

  // -- reading -----------------------------------------------------------------------------------
  
  @Override
  public JsonRecord read(DataInput in, JsonValue target) throws IOException
  {
    // read the size information
    if (noOptional > 0)
    {
      in.readFully(optionalBits.bytes);
    }
    int noAdditional = 0;
    if (additionalSerializer != null)
    {
      noAdditional = BaseUtil.readVUInt(in);
    }

    // obtain a record with enough capacity
    BufferedJsonRecord t;
    if (target instanceof BufferedJsonRecord)
    {
      t = (BufferedJsonRecord)target;
      t.ensureCapacity(noAdditional+noRequiredOptional);
    }
    else
    {
      t = new BufferedJsonRecord(noAdditional+noRequiredOptional);
    }
    
    // the internal data structures will be reused
    JsonString[] names = t.getInternalNamesArray();
    JsonValue[] values = t.getInternalValuesArray();

    // read the data
    readAdditional(in, names, values, 0, noAdditional, false);    
    int noPresent = readRequiredOptional(in, names, values, noAdditional, optionalBits);
    int n = noAdditional + noPresent;
    
    // set it
    if (noAdditional != 0 || noPresent != 0) 
    {
      // we do not HAVE to sort the names/values in the record, but it is much cheaper to 
      // do that now because we can exploit the knowledge that the additional names and 
      // required/optional names are sorted among themselves
      merge(names, values, noAdditional, n);
      t.set(names, values, n, true);
    }
    else
    {
      t.set(names, values, n, true);      
    }

    // done
    return t;
  }
  
  /** Sorts the name array (and the corresponding values). This implementation makes use of the
   * fact that the intervals [0...noAdditional-1] and [noAdditional...n-1] are both sorted. */
  private void merge(JsonString[] names, JsonValue[] values, int noAdditional, int n)
  {
    NameValue temp = new NameValue(); // when temp.name != null, stores a value to be inserted
    int p0=0, p1=noAdditional; 
    while (p0<p1 && p1<n)
    {
      // check whether p0 reached a position that we marked unused
      if (names[p0]==null)
      {
        swap(names, values, p0, temp);
        // temp.name = null --> temp is unused
      }
      
      // put smallest of names[p0], names[p1], temp.name to position p0 
      int cmp = names[p0].compareTo(names[p1]);
      if (cmp < 0)
      {
        // p0 < p1
        if (temp.name != null && names[p0].compareTo(temp.name) > 0)
        {
          // temp < p0 < p1
          swap(names, values, p0, temp);
        }
        p0++;
      }
      else
      {
        // p1 < p0
        if (temp.name == null)
        {
          temp.name = names[p0];
          temp.value = values[p0];
          names[p0] = names[p1];
          values[p0] = values[p1];
          names[p1] = null; // mark as unused
          p1++;
        } 
        else if (names[p1].compareTo(temp.name) > 0)
        {
          // temp < p1 < p0
          swap(names, values, p0, temp);
        }
        else
        {
          // p1 < p0, p1 < temp 
          swap(names, values, p0, p1);
          p1++;
        }
        p0++;
      }
    }
    
    // insert temp when necessary
    if (names[p0] == null)
    {
      names[p0] = temp.name;
      values[p0] = temp.value;
    }     
  }
  
  /** Exchanges name/value pairs at positions i and j */
  private void swap(JsonString[] names, JsonValue[] values, int i, int j)
  {
    JsonString tn = names[i];
    JsonValue tv = values[i];
    names[i] = names[j];
    values[i] = values[j];
    names[j] = tn;
    values[j] = tv;
  }

  /** Exchanges name/value pair at positions i with t */
  private void swap(JsonString[] names, JsonValue[] values, int i, NameValue t)
  {
    JsonString tn = names[i];
    JsonValue tv = values[i];
    names[i] = t.name;
    values[i] = t.value;
    t.name = tn;
    t.value = tv;
  }

  /** Read <code>length</code> additional fields from the provided input stream into the 
   * <code>names</code> and <code>values</code> arrays at the specified <code>offset</code>. 
   * The arrays have to be sufficiently large. */
  private void readAdditional(DataInput in, JsonString[] names, JsonValue[] values, int offset, 
      int length, boolean reuseNames) 
  throws IOException
  {
    // read the additional fields
    if (additionalSerializer != null)
    {
      for (int i=0; i<length; i++)
      {
        int j = offset+i;
        // TODO: reuse 
        names[j] = (JsonString)nameSerializer.read(in, reuseNames ? names[j] : null);
        values[j] = additionalSerializer.read(in, values[j]);
      }
    }
  }
  
  /** Read required and optional fields from the provided input stream into the 
   * <code>names</code> and <code>values</code> arrays at the specified <code>offset</code>. 
   * <code>optionalBits</code> determines which optional fields are present. 
   * The arrays have to be sufficiently large. */
  private int readRequiredOptional(DataInput in, JsonString[] names, JsonValue[] values, int offset, 
      MyBitSet optionalBits) throws IOException
  {
    // read the optional and required fields
    int n = offset;
    for (int i=0; i<noRequiredOptional; i++)
    {
      FieldInfo info = allInfo[i];
      if (info.field.isOptional() && !optionalBits.get(info.index))
      {
        // optional field that is not present
        continue;
      }
      
      names[n] = info.name;
      values[n] = info.serializer.read(in, values[n]);
      n++;
    }
    return n-offset;
  }
  
  
  // -- writing -----------------------------------------------------------------------------------
  
  @Override
  public void write(DataOutput out, JsonRecord inRecord) throws IOException
  {
    // put all required, optional and additional fields in the corresponding lists. 
    partition(inRecord);

    // Write a bit list indicating which optional fields are present
    if (noOptional > 0)
    {
      out.write(optionalBits.bytes);
    }
    
    // Write the additional fields 
    if (additionalSerializer != null)
    {
      int n = additionalNames.size();
      BaseUtil.writeVUInt(out, n);
      Iterator<JsonString> nameIt = additionalNames.iterator();
      Iterator<JsonValue> valueIt = additionalValues.iterator();
      while (nameIt.hasNext() && valueIt.hasNext())
      {
        JsonString name = nameIt.next();
        JsonValue value = valueIt.next();
        nameSerializer.write(out, name);
        additionalSerializer.write(out, value);
      }
    }
    
    // Write the required and optional fields intermingled
    int nextOptional = 0;
    for (int i=0; i<noRequiredOptional; i++)
    {
      FieldInfo info = allInfo[i];
      
      // required or optional
      if (info.field.isOptional())
      {
        // optional field; check if present
        if (optionalBits.get(info.index))
        {
          // yes, it is --> write it
          info.serializer.write(out, optionalValues[nextOptional]);
          ++nextOptional;
        }
      }
      else
      {
        // required field, write always
        info.serializer.write(out, requiredValues[info.index]);
      }
    }
    
    // puh! done
  }
  
  /** Partitions the fields in <code>inRecords</code> into required, optional, and additional
   * fields. Adds the value of all required fields to <code>requiredValues</code>, set the
   * bit list <code>optionalFields</code> describing which optional fields are present, adds 
   * the values of all present optional fields to <code>optionalValues</code>, and adds the name 
   * and values of all additional fields to <code>optionalNames</code> and 
   * <code>additionalValues</code>, respectively.
   */
  private void partition(JsonRecord inRecord)
  {
    // init
    optionalBits.clear();
    additionalNames.clear();
    additionalValues.clear();
    
    // set up iterators
    int posSchema = 0;
    FieldInfo schema = posSchema < noRequiredOptional ? allInfo[posSchema] : null;
    Iterator<Entry<JsonString, JsonValue>> recordIt = inRecord.iteratorSorted();
    Entry<JsonString, JsonValue> record = recordIt.hasNext() ? recordIt.next() : null;
    
    // scan schema and record concurrently (both are sorted by field name)
    int posRequired = 0, posOptional = 0, nextOptional = 0;
    while (record != null && schema != null)
    {
      // there are more fields
      int cmp = JsonUtil.compare(schema.field.getName(), record.getKey());
      if (cmp == 0)
      {
        // identical field names
        if (!schema.field.isOptional())
        {
          requiredValues[posRequired] = record.getValue();
          ++posRequired;
          ++posSchema;
          schema = posSchema < noRequiredOptional ? allInfo[posSchema] : null;
          record = recordIt.hasNext() ? recordIt.next() : null;
        }
        else
        {
          optionalBits.set(posOptional);
          optionalValues[nextOptional] = record.getValue();
          ++posOptional;
          ++nextOptional;
          ++posSchema;
          schema = posSchema < noRequiredOptional ? allInfo[posSchema] : null;
          record = recordIt.hasNext() ? recordIt.next() : null;
        }
      }
      else if (cmp < 0)
      {
        // schema field name comes first
        if (!schema.field.isOptional())
        {
          throw new IllegalArgumentException("field missing: " + schema.field.getName());
        }
        ++posOptional;
        ++posSchema;
        schema = posSchema < noRequiredOptional ? allInfo[posSchema] : null;
        continue;
      } 
      else 
      {
        // record field name comes first
        if (additionalSerializer == null)
        {
          throw new IllegalArgumentException("invalid field: " + record.getKey());
        }
        additionalNames.add(record.getKey());
        additionalValues.add(record.getValue());
        record = recordIt.hasNext() ? recordIt.next() : null;
      }
    }
    
    // process remaining fields in record
    while (record != null) // if so, schema must be null
    {
      if (additionalSerializer == null)
      {
        throw new IllegalArgumentException("invalid field: " + record.getKey());
      }
      additionalNames.add(record.getKey());
      additionalValues.add(record.getValue());
      record = recordIt.hasNext() ? recordIt.next() : null;
    }
    
    // At this point, all the fields in "record" have been processed. Now check that there are no 
    // required fields that were missing in "record"
    while (schema != null)
    {
      if (!schema.field.isOptional())
      {
        throw new IllegalArgumentException("missing field: " + schema.field.getName());
      }
      ++posOptional;
      ++posSchema;
      schema = posSchema < noRequiredOptional ? allInfo[posSchema] : null;
    }
    assert posRequired == noRequired;
    assert posOptional == noOptional;
    assert additionalNames.size() == additionalValues.size();
    assert !(additionalSerializer == null && additionalNames.size() > 0);
  }
  

  // -- comparison --------------------------------------------------------------------------------
  
  public int compare(DataInput in1, DataInput in2) throws IOException {
    // special case: all fields are additional
    if (noRequiredOptional == 0)
    {
      return additionalOnlyCompare(in1, in2);
    }

    // determine which optional fields are present in both streams 
    if (noOptional > 0)
    {
      in1.readFully(optionalBits1.bytes);
      in2.readFully(optionalBits2.bytes);
    }

    // deserialize the additional fields 
    int noAdditional1 = 0;
    int noAdditional2 = 0;
    if (additionalSerializer != null)
    {
      // read counts
      noAdditional1 = BaseUtil.readVUInt(in1);
      noAdditional2 = BaseUtil.readVUInt(in2);
      
      // resize data structure
      ensureCacheCapacity1(noAdditional1);
      ensureCacheCapacity2(noAdditional2);
      
      // and read
      readAdditional(in1, additionalNamesCache1, additionalValuesCache1, 0, noAdditional1, true);
      readAdditional(in2, additionalNamesCache2, additionalValuesCache2, 0, noAdditional2, true);
    }
    
    // start the comparison process
    int posAdditional = 0;
    for (int i=0; i<noRequiredOptional; i++) // there is at least one
    {
      FieldInfo info = allInfo[i];
      
      // check if one of the additional fields is smallest
      while (posAdditional < noAdditional1 || posAdditional < noAdditional2)
      {
        // get the names
        JsonString name1 = posAdditional < noAdditional1 ? additionalNamesCache1[posAdditional] : null;
        JsonString name2 = posAdditional < noAdditional2 ? additionalNamesCache2[posAdditional] : null;
        
        // check if they are smallest
        boolean name1next = name1!=null && name1.compareTo(info.field.getName()) < 0; // cannot be ==
        boolean name2next = name2!=null && name2.compareTo(info.field.getName()) < 0; // cannot be ==

        // if they are different, we found a difference
        if (name1next)
        {
          if (!name2next)
          {
            // in1 has the smaller field name
            return -1;
          }
        }
        else if (name2next)
        {
          // in2 has the smaller field name
          return +1;
        }
        else
        {
         // the field in info (required or optional) is next
          break;
        }
        
        // compare the values
        assert name1next && name2next;
        JsonValue value1 = additionalValuesCache1[posAdditional];
        JsonValue value2 = additionalValuesCache2[posAdditional];
        int cmp = JsonUtil.compare(value1, value2);      
        if (cmp != 0)
        {
          return cmp;
        }
        ++posAdditional;
      }
      
      
      // if we reach this point, the field in info is next
      // first check if it is optional, and if so, if it is in both records
      if (info.field.isOptional())
      {
        boolean fieldIn1 = optionalBits1.get(info.index);
        boolean fieldIn2 = optionalBits2.get(info.index);
        if (fieldIn1)
        {
          if (!fieldIn2)
          {
            // in1 has the smaller field name
            return -1;
          }
        }
        else if (fieldIn2)
        {
          // in2 has the smaller field name
          return +1;
        }
        else
        {
          // field not present in both streams
          continue;
        }
        assert fieldIn1 && fieldIn2;
      }

      // compare values
      int cmp = info.serializer.compare(in1, in2);
      if (cmp != 0) return cmp;      
    }
    
    // compare the remaining additional fields
    while (posAdditional < noAdditional1 && posAdditional < noAdditional2)
    {
      // compare the names
      JsonString name1 = additionalNamesCache1[posAdditional];
      JsonString name2 = additionalNamesCache2[posAdditional];
      int cmp = name1.compareTo(name2);
      if (cmp != 0) return cmp;
      
      
      // compare the values
      JsonValue value1 = additionalValuesCache1[posAdditional];
      JsonValue value2 = additionalValuesCache2[posAdditional];
      cmp = JsonUtil.compare(value1, value2);      
      if (cmp != 0) return cmp;
      
      ++posAdditional;
    }
    
    // at this point both have the same prefix fields, but one has more fields (this one is larger)
    return noAdditional1-noAdditional2;
  }
  
  /** Comparison when all fields are additional */
  public int additionalOnlyCompare(DataInput in1, DataInput in2) throws IOException {
    if (additionalSerializer == null)
    {
      // both empty
      return 0;      
    }
    
    // determine lengths
    int noAdditional1 = BaseUtil.readVUInt(in1);
    int noAdditional2 = BaseUtil.readVUInt(in2);
    
    // go
    int n = Math.min(noAdditional1, noAdditional2);
    for (int i = 0; i<n; i++)
    {
      // compare the names
      int cmp = nameSerializer.compare(in1, in2);
      if (cmp != 0) return cmp;
      
      // read the values
      cmp = additionalSerializer.compare(in1, in2);
      if (cmp != 0) return cmp;
    }
    
    // at this point both have the same prefix fields, but one has more fields (this one is larger)
    return noAdditional1-noAdditional2;
  }
  
  /** Ensure that the <code>additionalNamesCache1</code> and <code>additionalValuesCache2</code> 
   * arrays have at least size <code>n</code>. */
  private void ensureCacheCapacity1(int n)
  {
    if (additionalNamesCache1.length < n)
    {
      JsonString[] s = new JsonString[n];
      System.arraycopy(additionalNamesCache1, 0, s, 0, additionalNamesCache1.length);
      additionalNamesCache1 = s;
      JsonValue[] v = new JsonValue[n];
      System.arraycopy(additionalValuesCache1, 0, v, 0, additionalValuesCache1.length);
      additionalValuesCache1 = v;
    }
  }
  
  /** Ensure that the <code>additionalNamesCache2</code> and <code>additionalValuesCache2</code> 
   * arrays have at least size <code>n</code>. */
  private void ensureCacheCapacity2(int n)
  {
    if (additionalNamesCache2.length < n)
    {
      JsonString[] s = new JsonString[n];
      System.arraycopy(additionalNamesCache2, 0, s, 0, additionalNamesCache2.length);
      additionalNamesCache2 = s;
      JsonValue[] v = new JsonValue[n];
      System.arraycopy(additionalValuesCache2, 0, v, 0, additionalValuesCache2.length);
      additionalValuesCache2 = v;
    }
  }
  
  //TODO: efficient implementation of skip, and copy

}

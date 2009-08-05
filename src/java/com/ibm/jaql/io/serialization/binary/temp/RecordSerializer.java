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
  private static final JsonString[] NO_NAMES = new JsonString[0];
  private static final JsonValue[] NO_VALUES = new JsonValue[0];
  
  private RecordSchema schema;
  private TempBinaryFullSerializer stringSerializer;
  private TempBinaryFullSerializer restSerializer;
  
  private enum TYPE { ADDITIONAL, OPTIONAL, REQUIRED };
  
  // info variables
  int noRequired;
  int noOptional;
  int noRequiredOrOptional;
  private List<FieldInfo> requiredInfo;
  private List<FieldInfo> optionalInfo; 
  private List<FieldInfo> allInfo;
  
  // worker variables for serialization and deserialization
  private List<JsonValue> requiredValues = new ArrayList<JsonValue>();
  private MyBitSet optionalFields;
  private List<JsonValue> optionalValues = new ArrayList<JsonValue>();
  private List<JsonString> additionalNames = new ArrayList<JsonString>();
  private List<JsonValue> additionalValues = new ArrayList<JsonValue>();
  
  // worker variables for binary comparison
  private List<JsonString> additionalNames1 = new ArrayList<JsonString>();
  private List<JsonValue> additionalValues1 = new ArrayList<JsonValue>();
  private List<JsonString> additionalNames2 = new ArrayList<JsonString>();
  private List<JsonValue> additionalValues2 = new ArrayList<JsonValue>();
  private MyBitSet optionalFields1;
  private MyBitSet optionalFields2;
  
  private static class FieldInfo
  {
    RecordSchema.Field field;
    TempBinaryFullSerializer serializer;
    int index; // index in requiredInfo or optionalInfo
    
    FieldInfo(RecordSchema.Field field, TempBinaryFullSerializer serializer)
    {
      this.field = field;
      this.serializer = serializer;
    }
  }
  
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
  
  // -- construction ------------------------------------------------------------------------------
  
  public RecordSerializer(RecordSchema schema)
  {
    this.schema = schema; 
    init();
  }
  
  private void init()
  {
    // scan fields
    noOptional = 0;
    noRequired = 0;
    optionalInfo = new ArrayList<FieldInfo>();
    requiredInfo = new ArrayList<FieldInfo>();
    allInfo = new ArrayList<FieldInfo>();
    for (RecordSchema.Field f : schema.getFields())
    {
      FieldInfo k = new FieldInfo(f, new TempBinaryFullSerializer(f.getSchema()));
      allInfo.add(k);
      if (f.isOptional()) {
        optionalInfo.add(k);
        k.index = optionalInfo.size()-1;
        ++noOptional;
        ++noRequiredOrOptional;
      }
      else
      {
        requiredInfo.add(k);
        k.index = requiredInfo.size()-1;
        ++noRequired;
        ++noRequiredOrOptional;
      }
    }
    optionalFields = new MyBitSet(noOptional);
    optionalFields1 = new MyBitSet(noOptional);
    optionalFields2 = new MyBitSet(noOptional);
    
    // scan rest
    if (schema.getRest() != null)
    {
      restSerializer = new TempBinaryFullSerializer(schema.getRest());
      stringSerializer = new TempBinaryFullSerializer(SchemaFactory.stringSchema());
    }
  }
  
  // -- serialization -----------------------------------------------------------------------------
  
  @Override
  public JsonRecord newInstance()
  {
    // TODO: make type dependent on schema?
    return new BufferedJsonRecord();
  }

  @Override
  public JsonRecord read(DataInput in, JsonValue target) throws IOException
  {
    BufferedJsonRecord t;
    JsonString[] reusableNames = NO_NAMES;
    JsonValue[] reusableValues = NO_VALUES; 
    if (target instanceof BufferedJsonRecord)
    {
      t = (BufferedJsonRecord)target;
      reusableNames = t.getInternalNamesArray();
      reusableValues = t.getInternalValuesArray();
      t.clear(); 
    }
    else
    {
      t = new BufferedJsonRecord();
    }
    int k = 0;                               // position of next reused name/value instance
    int noReusable = reusableNames.length;   // number of those instances
    
    try 
    {
      // read the additional fields
      if (restSerializer != null)
      {
        int n = BaseUtil.readVUInt(in);
        for (int i=0; i<n; i++)
        {
          JsonString name;
          JsonValue value;
          if (k<noReusable)
          {
            name = (JsonString)stringSerializer.read(in, reusableNames[k]);
            value = restSerializer.read(in, reusableValues[k]);
            k++;
          }
          else
          {
            name = (JsonString)stringSerializer.read(in, null);
            value = restSerializer.read(in, null);
          };
          t.add(name, value);
        }
      }

      // determine which optional fields are present
      if (noOptional > 0)
      {
        in.readFully(optionalFields.bytes);
      }
      
      // read the optional and required fields
      for (int i=0; i<noRequiredOrOptional; i++)
      {
        FieldInfo info = allInfo.get(i);
        if (info.field.isOptional() && !optionalFields.get(info.index))
        {
          // optional field that is not present
          continue;
        }
        
        JsonString name;
        JsonValue value;
        if (k<noReusable)
        {
          name = JsonUtil.getCopy(info.field.getName(), reusableNames[k]);
          value = info.serializer.read(in, reusableValues[k]);
          k++;
        }
        else
        {
          name = JsonUtil.getCopy(info.field.getName(), null);
          value = info.serializer.read(in, null);
        }
        t.add(name, value);
      }
      
      // done
      return t;
    } 
    catch (Exception e)
    {
      throw new IOException(e);
    }
  }

  @Override
  public void write(DataOutput out, JsonRecord inRecord) throws IOException
  {
    // put all required, optional and additional fields are in the corresponding lists. 
    partition(inRecord);

    // Write the additional fields first
    if (restSerializer != null)
    {
      int n = additionalNames.size();
      BaseUtil.writeVUInt(out, n);
      Iterator<JsonString> nameIt = additionalNames.iterator();
      Iterator<JsonValue> valueIt = additionalValues.iterator();
      while (nameIt.hasNext() && valueIt.hasNext())
      {
        JsonString name = nameIt.next();
        JsonValue value = valueIt.next();
        stringSerializer.write(out, name);
        restSerializer.write(out, value);
      }
    }

    // Write a bit list indicating which optional fields are present
    if (noOptional > 0)
    {
      out.write(optionalFields.bytes);
    }
    
    // Write the required and optional fields intermingled
    int nextOptional = 0;
    for (int i=0; i<noRequiredOrOptional; i++)
    {
      FieldInfo info = allInfo.get(i);
      
      // required or optional
      if (info.field.isOptional())
      {
        // optional field; check if present
        if (optionalFields.get(info.index))
        {
          // yes, it is --> write it
          info.serializer.write(out, optionalValues.get(nextOptional));
          ++nextOptional;
        }
      }
      else
      {
        // required field, write always
        info.serializer.write(out, requiredValues.get(info.index));
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
    requiredValues.clear();
    optionalFields.clear();
    optionalValues.clear();
    additionalNames.clear();
    additionalValues.clear();
    
    // set up iterators
    Iterator<FieldInfo> schemaIt = allInfo.iterator();
    Iterator<Entry<JsonString, JsonValue>> recordIt = inRecord.iterator();
    FieldInfo schema = schemaIt.hasNext() ? schemaIt.next() : null;
    Entry<JsonString, JsonValue> record = recordIt.hasNext() ? recordIt.next() : null;
    
    // scan schema and record concurrently (both are sorted by field name)
    int currentOptional = 0;
    while (record != null)
    {
      TYPE type;
      if (schema == null) 
      {
        type = TYPE.ADDITIONAL;
      }
      else
      {
        // there are more fields
        int cmp = JsonUtil.compare(schema.field.getName(), record.getKey());
        if (cmp < 0)
        {
          // schema field name comes first
          if (!schema.field.isOptional())
          {
            throw new IllegalArgumentException("field missing: " + schema.field.getName());
          }
          schema = schemaIt.hasNext() ? schemaIt.next() : null;
          ++currentOptional;
          continue;
        } 
        else if (cmp > 0)
        {
          // record field name comes first
          type = TYPE.ADDITIONAL;
        }
        else
        {
          // identical field names
          if (!schema.field.isOptional())
          {
            type = TYPE.REQUIRED;
          }
          else
          {
            type = TYPE.OPTIONAL;
          }
        }
      }
       
      // handle field
      switch (type)
      {
      case REQUIRED:
        requiredValues.add(record.getValue());
        schema = schemaIt.hasNext() ? schemaIt.next() : null;
        record = recordIt.hasNext() ? recordIt.next() : null;
        break;
      case OPTIONAL:
        optionalFields.set(currentOptional);
        optionalValues.add(record.getValue());
        ++currentOptional;
        schema = schemaIt.hasNext() ? schemaIt.next() : null;
        record = recordIt.hasNext() ? recordIt.next() : null;
        break;
      case ADDITIONAL:
        if (restSerializer == null)
        {
          throw new IllegalArgumentException("invalid field: " + record.getKey());
        }
        additionalNames.add(record.getKey());
        additionalValues.add(record.getValue());
        record = recordIt.hasNext() ? recordIt.next() : null;
        break;
      }
    }
    
    // At this point, all the fields in "record" have been processed. Now check that there are no 
    // required fields that were missing in "record"
    while (schema != null)
    {
      if (!schema.field.isOptional())
      {
        throw new IllegalArgumentException("missing field: " + schema.field.getName());
      }
      ++currentOptional;
      schema = schemaIt.hasNext() ? schemaIt.next() : null;
    }
    assert requiredValues.size() == requiredInfo.size();
    assert currentOptional == noOptional;
    assert additionalNames.size() == additionalValues.size();
    assert !(restSerializer == null && additionalNames.size() > 0);
  }
  

  //TODO: efficient implementation of skip, and copy
  
  // -- comparison --------------------------------------------------------------------------------

  
  public int compare(DataInput in1, DataInput in2) throws IOException {
    // special case: all fields are additional
    if (noRequiredOrOptional == 0)
    {
      return additionalOnlyCompare(in1, in2);
    }
    
    // deserialize the additional fields 
    int noAdditional1 = 0;
    int noAdditional2 = 0;
    if (restSerializer != null)
    {
      // left side
      noAdditional1 = BaseUtil.readVUInt(in1);
      resizeTemps(noAdditional1, additionalNames1, additionalValues1);
      for (int i=0; i<noAdditional1; i++)
      {
        additionalNames1.set(i, (JsonString)stringSerializer.read(in1, additionalNames1.get(i)));
        additionalValues1.set(i, restSerializer.read(in1, additionalValues1.get(i)));
      }
      
      
      // right side
      noAdditional2 = BaseUtil.readVUInt(in2);
      resizeTemps(noAdditional2, additionalNames2, additionalValues2);
      for (int i=0; i<noAdditional2; i++)
      {
        additionalNames2.set(i, (JsonString)stringSerializer.read(in2, additionalNames2.get(i)));
        additionalValues2.set(i, restSerializer.read(in2, additionalValues2.get(i)));
      }
    }
    
    // now determine which optional fields are present in both streams 
    if (noOptional > 0)
    {
      in1.readFully(optionalFields1.bytes);
      in2.readFully(optionalFields2.bytes);
    }
    
    // start the comparison process
    int posAdditional = 0;
    for (int i=0; i<noRequiredOrOptional; i++) // there is at least one
    {
      FieldInfo info = allInfo.get(i);
      
      // check if additional fields are smaller
      while (posAdditional < noAdditional1 || posAdditional < noAdditional2)
      {
        // get the names
        JsonString name1 = posAdditional < noAdditional1 ? additionalNames1.get(posAdditional) : null;
        JsonString name2 = posAdditional < noAdditional2 ? additionalNames2.get(posAdditional) : null;
        
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
        JsonValue value1 = additionalValues1.get(posAdditional);
        JsonValue value2 = additionalValues2.get(posAdditional);
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
        boolean fieldIn1 = optionalFields1.get(info.index);
        boolean fieldIn2 = optionalFields2.get(info.index);
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
      // get the names
      JsonString name1 = additionalNames1.get(posAdditional);
      JsonString name2 = additionalNames2.get(posAdditional);
      int cmp = name1.compareTo(name2);
      if (cmp != 0) return cmp;
      ++posAdditional;
    }
    
    // at this point both have the same prefix fields, but one has more fields (this one is larger)
    return noAdditional1-noAdditional2;
  }
  
  /** comparison when all fields are additional */
  public int additionalOnlyCompare(DataInput in1, DataInput in2) throws IOException {
    if (restSerializer == null)
    {
      // both empty
      return 0;      
    }
    
    // determine lengths
    int noAdditional1 = BaseUtil.readVUInt(in1);
    resizeTemps(noAdditional1, additionalNames1, additionalValues1);
    int noAdditional2 = BaseUtil.readVUInt(in2);
    resizeTemps(noAdditional2, additionalNames2, additionalValues2);
    
    // go
    int n = Math.min(noAdditional1, noAdditional2);
    for (int i = 0; i<n; i++)
    {
      // read the names
      JsonString name1 = (JsonString)stringSerializer.read(in1, additionalNames1.get(i));
      additionalNames1.set(i, name1);
      JsonString name2 = (JsonString)stringSerializer.read(in2, additionalNames2.get(i));
      additionalNames2.set(i, name2);
      
      // compare them
      int cmp = name1.compareTo(name2);
      if (cmp != 0) return cmp;
      
      // read the values
      JsonValue value1 = restSerializer.read(in1, additionalValues1.get(i));
      additionalValues1.set(i, value1);
      JsonValue value2 = restSerializer.read(in2, additionalValues2.get(i));
      additionalValues2.set(i, value2);
      
      // compare them
      cmp = value1.compareTo(value2);
      if (cmp != 0) return cmp;
    }
    
    // at this point both have the same prefix fields, but one has more fields (this one is larger)
    return noAdditional1-noAdditional2;
  }
  
  // ensures that the length of the two arrays is at least minSize
  private void resizeTemps(int minSize, List<JsonString> names, List<JsonValue> values)
  {
    assert names.size() == values.size();
    while (minSize > names.size())
    {
      names.add(null);
      values.add(null);
    }
  }
}

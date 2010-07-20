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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.ibm.jaql.io.serialization.binary.perf.LazyRecordSerializer.FieldInfo;
import com.ibm.jaql.json.schema.Schema;


/** An in-memory {@link JsonRecord}. Name-based field access is implemented using a hash table.
 * In addition, this class provides functionality to access record fields by index. This is 
 * possible because the main data structure used by this record is a parallel array: one array 
 * storing the field names and one array storing the corresponding field values. As long as the 
 * record names are not modified, field indexes are stable and can be used to avoid the look-up 
 * cost of name-based accesses.
 * 
 * Quick record construction is supported via the various <code>setInternal</code> methods.  
 */
public class LazyJsonRecord extends JsonRecord {
  protected static final JsonString[] NOTHING = new JsonString[0];
  
  // names and values are parallel arrays with names being kept in arbitrary order
  // invariants: size<=names.length==values.length
  protected final JsonString[]     names;			 
  protected JsonValue[]      values = NOTHING;		   
  protected int              size   = 0;
  
  private byte[] data = null;
  private boolean[] optBits = null;

  // index structures (we could be more efficent by implementing our own hash maps) 
  protected Map<JsonString, Integer> hashIndex;
  
  private final Schema schema;
  private final FieldInfo[] info;

  // -- construction ------------------------------------------------------------------------------
  
  /** Constructs an empty in-memory JSON record */
  public LazyJsonRecord(Schema schema, FieldInfo[] info)
  {
	this.schema = schema;
	this.info = info;
    hashIndex = new HashMap<JsonString, Integer>();
    names = new JsonString[info.length];
  }
  
  public void set(byte[] data, boolean[] optBits) {
	  this.data = data;
	  this.optBits = optBits;
	  
	  int optIndex = 0;
	  int fieldIndex = 0;
	  
	  for (int i = 0; i < info.length; i++) {
		  if(!info[i].optional) {
			  names[fieldIndex++] = info[i].name;
		  }
		  else if(optBits[i]) {
			  names[fieldIndex++] = info[i].name;
			  optIndex++;
		  } else {
			  optIndex++;
		  }
	}
  }

  // -- reading -----------------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonRecord#containsKey(com.ibm.jaql.json.type.JsonString) */
  @Override
  public boolean containsKey(JsonString key)
  {
    return indexOf(key) >= 0;
  }
  
  /* @see com.ibm.jaql.json.type.JsonRecord#get(com.ibm.jaql.json.type.JsonString,
   *      com.ibm.jaql.json.type.JsonValue) */
  /* 
   * Does not work for additional fields
   */
  @Override
  public JsonValue get(JsonString name, JsonValue defaultValue)
  {
	 try {
		return info[0].serializer.read((DataInput) new ByteArrayInputStream(data), null);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	return null;
    //int index = indexOf(name);
    //return index >= 0 ? values[index] : defaultValue;
  }
  
  @Override
  public JsonRecord getImmutableCopy() throws Exception
  {
    return this;
  }
  
  @Override
  public JsonRecord getCopy(JsonValue target) throws Exception {
  	// TODO Auto-generated method stub
  	return null;
  }

  @Override
  public int size() {
  	// TODO Auto-generated method stub
  	return 0;
  }


  // -- index-based access ------------------------------------------------------------------------

  /** Searches this record for the specified field and returns its index. Returns a negative
   * number when the field name has not been found. */
  public int indexOf(JsonString name)
  {
  	Integer index = hashIndex.get(name);
  	return index==null ? -1 : index;
  }

  /** Clears this record, i.e., removes all its fields. */
  public void clear()
  {
    this.size = 0;
    hashIndex.clear();
  }
  
  // -- Iterable interface ------------------------------------------------------------------------
  
  /** Returns an iterator over the fields in this record (in index order). */
  @Override
  public Iterator<Entry<JsonString, JsonValue>> iterator()
  {
    return new Iterator<Entry<JsonString, JsonValue>>()
    {
      int i = 0;
      RecordEntry entry = new RecordEntry(); // reused
      
      @Override
      public boolean hasNext()
      {
        return i < size();
      }

      @Override
      public Entry<JsonString, JsonValue> next()
      {
        entry.name = names[i]; 
        entry.value = values[i];
        i++;
        return entry;
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException();        
      }      
    };
  }
  
  /** Returns an iterator over the fields in this record (in field-name order). */
  @Override
  public Iterator<Entry<JsonString, JsonValue>> iteratorSorted()
  {
	  return iterator();
  }

  // -- misc --------------------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonValue#getEncoding() */
  @Override
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.RECORD;
  }
}

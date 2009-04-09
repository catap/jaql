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
package com.acme.extensions.data;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.ibm.jaql.JaqlBaseTestCase;
import com.ibm.jaql.io.converter.FromItem;
import com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem;
import com.ibm.jaql.json.type.FixedJArray;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.MemoryJRecord;

/**
 * This converter is used to convert a text-based data file, with an optional header, into
 * an array of JArrays or JRecords. Given a value assumed to be of type o.a.h.io.Text,
 * the value is first tokenized according to a delimiter and converted to either
 * a JArray or a JRecord. If no header is provided, then the input line is converted to an array.
 * Otherwise, the header is used to construct record field names. The header is assumed to be an
 * array of field names; it can be provided by reading a file of such names or as a literal array. 
 */
public class FromDelimitConverter extends HadoopRecordToItem<WritableComparable, Writable> {
  
  private static final Log LOG             = LogFactory.getLog(FromDelimitConverter.class.getName());
  
  public static final String DELIMITER_NAME = "delimiter";
  public static final String HEADER_NAME = "header";
  
  private String delimitter = ",";
  private JArray header = null;
  
  @Override
  public void init(JRecord options)
  {
    if(options != null) {
      // 1. check for delimiter override
      Item arg = options.getValue(DELIMITER_NAME);
      if(!arg.isNull()) {
        delimitter = arg.get().toString();
      }
      
      // 2. check for header
      arg = options.getValue(HEADER_NAME);
      if(!arg.isNull()) {
        try {
          header = (JArray) arg.get();
        } catch(Exception e) {
          LOG.info("exception with header: " + arg + "," + e);
        }
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem#createKeyConverter()
   */
  @Override
  protected FromItem<WritableComparable> createKeyConverter()
  {
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem#createValConverter()
   */
  @Override
  protected FromItem<Writable> createValConverter()
  {
    return new FromItem<Writable>() {
      public void convert(Writable src, Item tgt)
      {
        if (src == null || tgt == null) return;
        Text t = null;
        if (src instanceof Text)
        {
          t = (Text) src;
        }
        else
        {
          throw new RuntimeException("tried to convert from: " + src);
        }
        
        String[] vals = new String(t.getBytes()).split(delimitter);
        try {
          if(header == null) {
            setArray(vals, (FixedJArray)tgt.get());
          } else {
            setRecord(vals, header, (MemoryJRecord)tgt.get());
          }
        } catch(Exception e) {
          throw new RuntimeException(e);
        }
      }
      
      private void setRecord(String[] vals, JArray names, MemoryJRecord tgt) throws Exception
      {
        int n = (int) names.count();
        if(n != vals.length) 
          throw new RuntimeException("values and header disagree in length: " + vals.length + "," + n);
        
        for(int i = 0; i < n; i++) {
          JString name = (JString) names.nth(i).get();
          ((JString)tgt.getRequired(name.toString()).getNonNull()).set(vals[i]);
        }
      }
      
      private void setArray(String[] vals, FixedJArray tgt) {
        tgt.clear();
        int n = vals.length;
        for(int i = 0; i < n; i++) {
          tgt.add(new JString(vals[i])); // FIXME: memory
        }
      }
      
      public Item createTarget()
      {
        if(header == null)
          return new Item(new FixedJArray());
        else {
          int n = (int)header.count();
          MemoryJRecord r = new MemoryJRecord(n);
          try {
            for(int i = 0; i < n; i++) {
              r.add( (JString)header.nth(i).getNonNull(), new JString());
            }
          } catch(Exception e) { throw new RuntimeException(e);}

          return new Item(r);
        }
      }
    };
  }
}
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
package com.acme.extensions.fn;

import com.ibm.jaql.json.type.FixedJArray;
import com.ibm.jaql.json.type.JBinary;
import com.ibm.jaql.json.type.JBool;
import com.ibm.jaql.json.type.JDate;
import com.ibm.jaql.json.type.JDecimal;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JNumber;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.type.MemoryJRecord;
import com.ibm.jaql.json.type.SpillJArray;
import com.ibm.jaql.json.util.JIterator;
import com.ibm.jaql.lang.core.JFunction;

/**
 * 
 */
public class EveryType
{
  /**
   * @param vals
   * @return
   * @throws Exception
   */
  public JIterator eval(final JIterator vals) throws Exception
  {
    if (vals == null)
    {
      return null;
    }

    return new JIterator() {
      // These are both types and encodings
      MemoryJRecord rec  = new MemoryJRecord();
      JBool         bool = new JBool();
      JString       str  = new JString();
      JBinary       bin  = new JBinary();
      JDate         date = new JDate();
      JFunction     fn   = new JFunction();

      // These are encodings of JArray
      SpillJArray   arrs = new SpillJArray();
      FixedJArray   arrf = new FixedJArray();

      // These are encodings of JNumber
      JDecimal      dec  = new JDecimal();
      JLong         lng  = new JLong();

      public boolean moveNext() throws Exception
      {
        if (!vals.moveNext())
        {
          return false;
        }
        JValue v = vals.current();
        if (v == null)
        {
          // Just return nulls
          current = null;
        }
        else if (v instanceof SpillJArray)
        {
          // Keep every other entry of the array
          SpillJArray a = (SpillJArray) v;
          arrs.clear();
          JIterator iter = a.jiterator();
          boolean addit = true;
          while (iter.moveNext())
          {
            if (addit)
            {
              arrs.add(iter.current());
            }
            addit = !addit;
          }
          current = arrs;
        }
        else if (v instanceof FixedJArray)
        {
          // Reverse the array
          FixedJArray a = (FixedJArray) v;
          int n = a.size();
          arrf.resize(n);
          for (int i = 0; i < n; i++)
          {
            arrf.set(i, a.get(n - 1 - i));
          }
          current = arrf;
        }
        else if (v instanceof JRecord)
        {
          // Add "my_" before all the names of the record
          JRecord r = (JRecord) v;
          rec.clear();
          for (int i = 0; i < r.arity(); i++)
          {
            JString oldName = r.getName(i);
            v = r.getJValue(i);
            String nm = "my_" + oldName;
            rec.add(nm, v);
          }
          current = rec;
        }
        else if (v instanceof JBool)
        {
          // Negate bools
          JBool b = (JBool) v;
          bool.setValue(!b.getValue());
          current = bool;
        }
        else if (v instanceof JString)
        {
          // Add "hi, " before JStrings
          JString s = (JString) v;
          str.set("hi, " + s);
          current = str;
        }
        else if (v instanceof JNumber)
        {
          // Set a number to the negated or floor the value (and muck with encodings)
          JNumber n = (JNumber) v;
          if (n instanceof JDecimal)
          {
            lng.setValue(n.longValue());
            current = lng;
          }
          else if (n instanceof JLong)
          {
            dec.setValue(-n.longValue());
            current = dec;
          }
        }
        else if (v instanceof JBinary)
        {
          // Add 0xABADDEED before JBinary
          JBinary b = (JBinary) v;
          byte[] bytes1 = b.getBytes(); // Don't modify this!  v, b, bytes1 are not ours!
          int n = b.getLength();
          bin.ensureCapacity(n + 4);
          byte[] bytes2 = bin.getBytes(); // This can be modifed because bin is ours.
          bytes2[0] = (byte) 0xAB;
          bytes2[1] = (byte) 0xAD;
          bytes2[2] = (byte) 0xDE;
          bytes2[3] = (byte) 0xED;
          System.arraycopy(bytes1, 0, bytes2, 4, n);
          bin.setBytes(bytes2, n + 4);
          current = bin;
        }
        else if (v instanceof JDate)
        {
          // Add one hour to the date
          JDate d = (JDate) v;
          date.setMillis(d.getMillis() + 60 * 60 * 1000);
          current = date;
        }
        else if (v instanceof JFunction)
        {
          // TODO: Java functions need the context to be evaluated, and take Item arguments...
          JFunction f = (JFunction) v;
          fn.setCopy(f);
          current = f;
        }
        else
        {
          throw new RuntimeException("unknown type: " + v.getClass().getName());
        }
        return true;
      }
    };
  }
}
